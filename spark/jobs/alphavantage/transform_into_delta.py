# transform_into_delta.py
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, current_timestamp, to_timestamp, input_file_name, row_number, lit, max as spark_max, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from delta.tables import DeltaTable

# === CẤU HÌNH ===
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET = "warehouse"

SRC_PATH = f"s3a://{BUCKET}/bronze1/stocks_intraday/"
DST_PATH = f"s3a://{BUCKET}/bronze2/stocks_intraday"
CHECKPOINT_PATH = f"s3a://{BUCKET}/_checkpoints/bronze1_to_bronze2"

# Tên Database và Table cho Trino
DB_NAME = "bronze2"
TBL_NAME = "stocks_intraday"

def build_spark():
    return (SparkSession.builder
            .appName("bronze1_to_bronze2_ids")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.sql.catalogImplementation", "hive") 
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") 
            .getOrCreate())

def get_next_bronze_id(spark):
    try:
        if DeltaTable.isDeltaTable(spark, DST_PATH):
            max_id_row = spark.read.format("delta").load(DST_PATH).agg(spark_max("cd_bronze_id")).collect()[0][0]
            return int(max_id_row) if max_id_row is not None else 0
        return 0
    except:
        return 0

def process_batch(batch_df, batch_id):
    spark = batch_df.sparkSession
    
    if batch_df.rdd.isEmpty():
        return

    # 1. Chuyển đổi kiểu dữ liệu
    # === 🛠️ ĐÃ SỬA TẠI ĐÂY ===
    df_typed = (batch_df
        .withColumn("timestamp", col("timestamp").cast("timestamp"))
        
        .withColumn("open", col("open").cast("double"))
        .withColumn("high", col("high").cast("double"))
        .withColumn("low", col("low").cast("double"))
        .withColumn("close", col("close").cast("double"))
        .withColumn("volume", col("volume").cast("long"))
        .withColumn("source_file", input_file_name())
    )

    # 2. Tạo cột ID
    current_max_id = get_next_bronze_id(spark)
    w = Window.orderBy("timestamp", "symbol")
    
    df_final = (df_typed
        .withColumn("row_num", row_number().over(w))
        .withColumn("cd_bronze_id", col("row_num") + lit(current_max_id))
        .withColumn("dt_record_to_bronze", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        .drop("row_num")
    )

    # 3. Ghi dữ liệu xuống MinIO
    (df_final.write
        .format("delta")
        .mode("append")
        .partitionBy("date_ny")
        .option("mergeSchema", "true")
        .save(DST_PATH)
    )

def register_table_in_metastore(spark):
    """Đăng ký bảng với Hive Metastore để Trino nhìn thấy"""
    print(f"📝 Registering table {DB_NAME}.{TBL_NAME} in Metastore...")
    
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DB_NAME}.{TBL_NAME}
        USING DELTA
        LOCATION '{DST_PATH}'
    """)
    print("✅ Table registered successfully!")

def main():
    spark = build_spark()
    
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("symbol", StringType(), True),
        StructField("source", StringType(), True),
        StructField("date_ny", StringType(), True),
        StructField("dt_utc_ingest", StringType(), True)
    ])

    print(f"🚀 Processing stream from {SRC_PATH}...")
    
    df_stream = (spark.readStream
                 .format("json")
                 .schema(schema)
                 .option("recursiveFileLookup", "true") 
                 .load(SRC_PATH))

    query = (df_stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    
    register_table_in_metastore(spark)
    
    print("✅ Bronze 1 -> Bronze 2 Done (Table Registered)!")
    spark.stop()

if __name__ == "__main__":
    main()