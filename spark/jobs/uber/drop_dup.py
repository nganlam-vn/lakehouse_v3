"""
Script: Clean Duplicates in Silver Table
Action: Read Silver -> Deduplicate -> Overwrite Silver
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- CẤU HÌNH ---
DB_NAME = "silver"
TABLE_NAME = os.getenv("TABLE_NAME", "uber_bookings")

WAREHOUSE_LOCATION = "s3a://warehouse/"
METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")

if __name__ == "__main__":
    print(f"--- Starting Cleanup Job for: {DB_NAME}.{TABLE_NAME} ---")

    spark = (
        SparkSession.builder
        .appName("clean_duplicates_silver")
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
        .enableHiveSupport()
        .getOrCreate()
    )

    try:
        # 1. Đọc dữ liệu hiện tại
        full_table_name = f"{DB_NAME}.{TABLE_NAME}"
        if not spark.catalog.tableExists(full_table_name):
            print(f"Table {full_table_name} does not exist. Nothing to clean.")
            sys.exit(0)

        print(f"Reading table {full_table_name}...")
        df = spark.read.table(full_table_name)
        
        count_before = df.count()
        print(f"-> Rows before cleaning: {count_before}")

        # 2. Xóa Trùng lặp (Deduplicate)
        # LỰA CHỌN:
        # subset=["Booking_ID"]: Chỉ giữ 1 dòng duy nhất cho mỗi mã chuyến đi.
        # Nếu không dùng subset (để trống), nó chỉ xóa nếu TOÀN BỘ các cột giống y hệt nhau.
        
        print("--- Deduplicating based on 'Booking_ID' ---")
        # Lưu ý: Vì cột ID trong Silver đã được chuẩn hóa là 'Booking_ID' (có gạch dưới)
        if "Booking_ID" in df.columns:
            df_clean = df.dropDuplicates(subset=["Booking_ID"])
        else:
            # Fallback nếu chưa đổi tên cột
            print("Warning: 'Booking_ID' column not found. Using dropDuplicates on all columns.")
            df_clean = df.dropDuplicates()

        count_after = df_clean.count()
        print(f"-> Rows after cleaning: {count_after}")
        print(f"-> Removed: {count_before - count_after} rows.")

        if count_before == count_after:
            print("No duplicates found. Skipping write.")
        else:
            # 3. Ghi đè (Overwrite) lại bảng
            print("--- Overwriting Silver Table ---")
            
            # Quan trọng: Cần option mergeSchema hoặc overwriteSchema để đảm bảo an toàn
            write_opts = (
                df_clean.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .option("path", f"{WAREHOUSE_LOCATION}{DB_NAME}/{TABLE_NAME}")
            )
            
            if "Date" in df_clean.columns:
                write_opts.partitionBy("Date").saveAsTable(full_table_name)
            else:
                write_opts.saveAsTable(full_table_name)
            
            print("Overwrite complete.")

            # 4. Dọn dẹp file rác (Vacuum)
            # Vì Delta Lake lưu lịch sử, lệnh overwrite chưa xóa file cũ ngay.
            # Lệnh VACUUM sẽ xóa các file không còn dùng đến.
            print("--- Vacuuming old files ---")
            spark.sql(f"VACUUM {full_table_name} RETAIN 0 HOURS") 
            # Lưu ý: Mặc định Delta chặn RETAIN 0. Nếu lỗi, cần set config:
            # spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            
            print("Vacuum complete.")

        print("--- SUCCESS: Table is clean! ---")

    except Exception as e:
        print(f"--- ERROR: {e}")
        # import traceback
        # traceback.print_exc()
    finally:
        spark.stop()