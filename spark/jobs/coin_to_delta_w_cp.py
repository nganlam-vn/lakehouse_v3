from pyspark.sql import SparkSession
from pyspark.sql.functions import (input_file_name, regexp_extract, to_timestamp, 
                                    col, current_timestamp, to_date, concat_ws, 
                                    lit, sha2, monotonically_increasing_id)
from datetime import datetime

BUCKET = "warehouse"
PREFIX = "bronze1/coinmarketcap"

DST_DB  = "bronze2"
DST_TBL = "coinmarketcap"
DST_PATH = f"s3a://{BUCKET}/bronze2/{DST_TBL}"
CTRL_DB = "_control"
CTRL_TBL = "coinmarketcap_ingest_log"
CTRL_PATH = f"s3a://{BUCKET}/_control/{CTRL_TBL}"

bronze1_dir = f"s3a://{BUCKET}/{PREFIX}"

def get_last_key(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CTRL_DB}")
    
    # Thử đọc trực tiếp từ Delta location trước
    try:
        # Kiểm tra xem Delta table có tồn tại ở location không
        df = spark.read.format("delta").load(CTRL_PATH)
        
        # Nếu đọc được, đảm bảo table được register
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {CTRL_DB}.{CTRL_TBL}
            USING DELTA
            LOCATION '{CTRL_PATH}'
        """)
        
        # Query last_key
        row = df.orderBy(col("run_at").desc()).limit(1).collect()
        return row[0]["last_key"] if row else None
        
    except Exception as e:
        # Nếu không đọc được (table chưa tồn tại), tạo mới
        print(f"Control table not found, creating new one. Error was: {e}")
        
        # Tạo empty DataFrame với schema
        empty_df = spark.createDataFrame([], schema="run_at timestamp, last_key string, last_ts timestamp")
        empty_df.write.format("delta").mode("overwrite").save(CTRL_PATH)
        
        # Register table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {CTRL_DB}.{CTRL_TBL}
            USING DELTA
            LOCATION '{CTRL_PATH}'
        """)
        
        return None

def list_new_keys(spark, last_key):
    """Sử dụng Spark để list files từ S3A thay vì MinIO client"""
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    
    # Lấy FileSystem và Path từ Java
    fs_class = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    path_class = sc._gateway.jvm.org.apache.hadoop.fs.Path
    
    # Tạo S3A path
    s3a_path = path_class(f"s3a://{BUCKET}/{PREFIX}/")
    fs = fs_class.get(s3a_path.toUri(), hadoop_conf)
    
    new_keys = []
    
    # List tất cả files trong thư mục
    if fs.exists(s3a_path):
        file_statuses = fs.listStatus(s3a_path)
        for file_status in file_statuses:
            file_path = file_status.getPath()
            # Lấy relative path (bỏ s3a://warehouse/ prefix)
            key = file_path.toString().replace(f"s3a://{BUCKET}/", "")
            
            # Chỉ lấy file .json và mới hơn last_key
            if key.endswith(".json") and (last_key is None or key > last_key):
                new_keys.append(key)
    
    return sorted(new_keys)

def main():
    # Get existing Spark session (created by spark-submit with all configs)
    spark = SparkSession.builder.appName("cmc_checkpoint_ingest").getOrCreate()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DST_DB}")

    last_key = get_last_key(spark)
    print(f"Last processed key: {last_key if last_key else 'None (first run)'}")
    
    keys = list_new_keys(spark, last_key)
    
    if not keys:
        print("No new files to process.")
        return

    # Tạo paths từ keys
    paths = [f"s3a://{BUCKET}/{k}" for k in keys]
    
    print(f"Processing {len(keys)} new files...")
    
    # Đọc tất cả JSON files
    df = spark.read.json(paths)

    # Thêm metadata columns
    df = (df.withColumn("dt_utc_record_to_bronze2", current_timestamp())
            .withColumn("cd_bronze2_id", monotonically_increasing_id())
            .withColumn("p_date", to_date(current_timestamp()))
         )
    
    # Ghi vào Delta Lake với partition
    df.write.format("delta").mode("append").partitionBy("p_date").save(DST_PATH)
    
    # Tạo bảng nếu chưa tồn tại
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DST_DB}.{DST_TBL} 
        USING DELTA 
        LOCATION '{DST_PATH}'
    """)
    
    # Set table properties (optional)
    try:
        spark.sql(f"ALTER TABLE {DST_DB}.{DST_TBL} SET TBLPROPERTIES ('delta.appendOnly'='true')")
    except Exception as e:
        print(f"Note: Could not set table properties: {e}")

    # Ghi checkpoint - lưu lại key cuối cùng đã xử lý
    latest_key = max(keys)
    now = datetime.utcnow()
    
    # Tạo checkpoint DataFrame với Python datetime values
    checkpoint_df = spark.createDataFrame(
        [(now, latest_key, now)],
        schema="run_at timestamp, last_key string, last_ts timestamp"
    )
    
    checkpoint_df.write.format("delta").mode("append").save(CTRL_PATH)

    print(f"Successfully appended {len(keys)} files.")
    print(f"Latest checkpoint key: {latest_key}")

if __name__ == "__main__":
    main()