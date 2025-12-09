#csv_to_delta.py
import os
import sys
import traceback
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

# --- CẤU HÌNH ---
TABLE_NAME = os.getenv("TABLE_NAME", "uber_bookings")
DATABASE_NAME = "bronze2" 

# Đường dẫn nguồn
SOURCE_PATH = os.getenv("CSV_PATH", "s3a://warehouse/bronze1/uber_booking/")

# Đường dẫn đích
WAREHOUSE_LOCATION = "s3a://warehouse/"
# [QUAN TRỌNG] Đường dẫn lưu trạng thái Checkpoint
CHECKPOINT_LOCATION = "s3a://warehouse/checkpoints/ingest_bronze2/"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")

def process_batch(df, batch_id):
    """
    Hàm xử lý cho từng lô dữ liệu mới (Micro-batch).
    Spark sẽ tự động gọi hàm này mỗi khi tìm thấy file mới.
    """
    print(f"--- Processing Batch ID: {batch_id} with {df.count()} records ---")
    
    if df.count() == 0:
        return

    # 1. FIX TÊN CỘT
    new_columns = [
        c.replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_').strip()
        for c in df.columns
    ]
    df = df.toDF(*new_columns)
    
    # 2. FIX SCHEMA (Ép kiểu Double)
    numeric_cols_to_fix = ["Avg_VTAT", "Avg_CTAT", "Booking_Value", "Ride_Distance"]
    for col in numeric_cols_to_fix:
        if col in df.columns:
            df = df.withColumn(col, 
                F.when(F.lower(F.trim(F.col(col))).isin("null", "none", ""), None)
                 .otherwise(F.col(col))
                 .cast(DoubleType())
            )

    if "Date" in df.columns:
        df = df.withColumn("Date", F.to_date(F.col("Date"), "yyyy-MM-dd"))
        
    # 3. LOGIC TẠO ID TĂNG DẦN
    # Cần tạo Spark Session mới bên trong hàm worker này để truy vấn Metadata
    spark = SparkSession.getActiveSession()
    
    current_max_id = 0
    try:
        # Đọc bảng đích để lấy Max ID hiện tại
        if spark.catalog.tableExists(f"{DATABASE_NAME}.{TABLE_NAME}"):
            # Đọc snapshot hiện tại của bảng Delta
            target_df = spark.read.table(f"{DATABASE_NAME}.{TABLE_NAME}")
            if "cd_bronze_id" in target_df.columns:
                max_row = target_df.agg({"cd_bronze_id": "max"}).collect()[0]
                if max_row[0] is not None:
                    current_max_id = max_row[0]
    except Exception as e:
        print(f"Warning getting Max ID: {e}")

    print(f"-> Batch {batch_id}: Generating IDs starting from {current_max_id + 1}")

    w = Window.orderBy(F.monotonically_increasing_id())
    df = df.withColumn("row_num_temp", F.row_number().over(w))
    df = df.withColumn("cd_bronze_id", F.col("row_num_temp") + F.lit(current_max_id).cast("long"))
    df = df.withColumn("dt_record_to_bronze2", F.current_timestamp())

    front_cols = ["cd_bronze_id", "dt_record_to_bronze2"]
    remaining = [c for c in df.columns if c not in front_cols and c != "row_num_temp"]
    df = df.select(*front_cols, *remaining)

    # 4. GHI VÀO BRONZE 2 (APPEND)
    print(f"-> Writing to {DATABASE_NAME}.{TABLE_NAME}")
    
    # Tạo DB nếu chưa có
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME} LOCATION '{WAREHOUSE_LOCATION}{DATABASE_NAME}'")
    
    write_opts = (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true") 
        .option("path", f"{WAREHOUSE_LOCATION}{DATABASE_NAME}/{TABLE_NAME}")
    )

    if "Date" in df.columns:
        write_opts.partitionBy("Date").saveAsTable(f"{DATABASE_NAME}.{TABLE_NAME}")
    else:
        write_opts.saveAsTable(f"{DATABASE_NAME}.{TABLE_NAME}")

if __name__ == "__main__":
    
    spark = (
        SparkSession.builder
        .appName("csv_to_delta_streaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.hive.metastore.uris", METASTORE_URI)
        .config("spark.sql.warehouse.dir", WAREHOUSE_LOCATION)
        # Bắt buộc để Stream đọc được CSV mà không cần khai báo schema cứng
        .config("spark.sql.streaming.schemaInference", "true")
        .enableHiveSupport() 
        .getOrCreate()
    )

    print(f"Starting Incremental Ingestion from: {SOURCE_PATH}")
    
    try:
        # [BƯỚC 1] ĐỌC STREAM (Thay vì read.csv)
        # Spark sẽ theo dõi thư mục này
        streaming_df = (
            spark.readStream
            .format("csv")
            .option("header", "true")
            .option("recursiveFileLookup", "true") # Đọc đệ quy các folder date=...
            .load(SOURCE_PATH)
        )

        # [BƯỚC 2] XỬ LÝ & GHI
        # trigger(availableNow=True): Chạy như Batch (xử lý hết file mới rồi tắt) chứ không chạy mãi mãi.
        # checkpointLocation: Nơi lưu trạng thái "đã đọc file nào".
        query = (
            streaming_df.writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", CHECKPOINT_LOCATION)
            .trigger(availableNow=True) 
            .start()
        )
        
        query.awaitTermination()
        print("--- SUCCESS: Incremental Job Completed ---")
        
    except Exception as e:
        print(f"--- ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        spark.stop()