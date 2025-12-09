#bronze2_to_silver.py
import os
import sys
import traceback
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

# --- CẤU HÌNH ---
SOURCE_DB = "bronze2"
TARGET_DB = "silver"
TABLE_NAME = os.getenv("TABLE_NAME", "uber_bookings")

WAREHOUSE_LOCATION = "s3a://warehouse/"
METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")

if __name__ == "__main__":
    print(f"--- Starting Silver Job: {SOURCE_DB} -> {TARGET_DB} ---")
    
    spark = (
        SparkSession.builder
        .appName("bronze_to_silver_etl")
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
        print(f"Reading from {SOURCE_DB}.{TABLE_NAME}...")
        df = spark.read.table(f"{SOURCE_DB}.{TABLE_NAME}")
        
        print("--- Checking for new data (Incremental Check) ---")
        last_processed_id = -1
        
        if spark.catalog.tableExists(f"{TARGET_DB}.{TABLE_NAME}"):
            try:
                silver_df = spark.read.table(f"{TARGET_DB}.{TABLE_NAME}")
                if "cd_bronze_id" in silver_df.columns:
                    max_row = silver_df.agg({"cd_bronze_id": "max"}).collect()[0]
                    if max_row[0] is not None:
                        last_processed_id = max_row[0]
            except Exception as e:
                print(f"Warning checking Silver table: {e}")

        print(f"-> Last Processed Bronze ID: {last_processed_id}")
        
        df = df.filter(F.col("cd_bronze_id") > last_processed_id)
        
        new_count = df.count()
        if new_count == 0:
            print(">>> No new data found. Skipping job.")
            sys.exit(0)
            
        print(f">>> Found {new_count} new records to process.")

        # 1. CLEANING & CASTING
        print("--- Sanitizing & Casting Numeric Columns ---")
        numeric_cols = ["Avg_VTAT", "Avg_CTAT", "Booking_Value", "Ride_Distance"]
        
        for col in numeric_cols:
            df = df.withColumn(col, 
                F.when(F.lower(F.trim(F.col(col))).isin("null", "none", "nan", ""), None)
                 .otherwise(F.col(col))
                 .cast(DoubleType())
            )
        print("--- Dropping Duplicates ---")
        # Logic: Giữ lại dòng đầu tiên tìm thấy của mỗi Booking ID
        # Nếu cột Booking_ID tồn tại thì dùng nó, không thì dropDuplicates() tất cả các cột
        if "Booking_ID" in df.columns:
            df = df.dropDuplicates(["Booking_ID"])
            print("-> Applied dropDuplicates on [Booking_ID]")
        else:
            df = df.dropDuplicates()
            print("-> Applied dropDuplicates on ALL columns (Booking_ID not found)")
            
        # 2. IMPUTATION
        print("--- Filling Null Values ---")
        
        df = df.fillna({
            "Incomplete_Rides_Reason": "Reason Unknown",
            "Driver_Cancellation_Reason": "Reason Unknown",
            "Reason_for_cancelling_by_Customer": "Reason Unknown",
            "Incomplete_Rides": 0,
            "Cancelled_Rides_by_Customer": 0,
            "Cancelled_Rides_by_Driver": 0
        })

        print("--- Calculating Means ---")
        stats = df.select([F.mean(c).alias(c) for c in numeric_cols]).collect()[0]
        means_dict = stats.asDict()
        
        for col in numeric_cols:
            mean_val = means_dict.get(col)
            fill_val = mean_val if mean_val is not None else 0.0
            df = df.fillna(fill_val, subset=[col])

        payment_mode_row = df.groupBy("Payment_Method").count().orderBy(F.desc("count")).first()
        if payment_mode_row and payment_mode_row["Payment_Method"]:
            df = df.fillna(payment_mode_row["Payment_Method"], subset=["Payment_Method"])

        # 3. FEATURE ENGINEERING
        print("--- Feature Engineering ---")
        
        df = df.withColumn("Hour_Int", F.hour(F.to_timestamp("Time", "HH:mm:ss")))

        df = df.withColumn("TimeZone", 
            F.when((F.col("Hour_Int") >= 6) & (F.col("Hour_Int") <= 12), "Morning")
             .when((F.col("Hour_Int") > 12) & (F.col("Hour_Int") <= 18), "Afternoon")
             .when((F.col("Hour_Int") > 18), "Evenings")
             .otherwise("LateNights")
        )

        df = df.withColumn("RushHour", 
            F.when((F.col("Hour_Int") >= 7) & (F.col("Hour_Int") <= 10), 1)
             .when((F.col("Hour_Int") >= 15) & (F.col("Hour_Int") <= 18), 1)
             .otherwise(0)
        )
        df = df.withColumnRenamed("Hour_Int", "Hour")

        # 4. SILVER ID GENERATION
        print("--- Generating Silver IDs ---")
        
        current_max_silver_id = 0
        try:
            if spark.catalog.tableExists(f"{TARGET_DB}.{TABLE_NAME}"):
                existing_df = spark.read.table(f"{TARGET_DB}.{TABLE_NAME}")
                if "cd_silver_id" in existing_df.columns:
                    max_row = existing_df.agg({"cd_silver_id": "max"}).collect()[0]
                    if max_row[0] is not None:
                        current_max_silver_id = max_row[0]
        except Exception:
            pass
        
        print(f"-> Start Silver ID from: {current_max_silver_id + 1}")

        w = Window.orderBy(F.monotonically_increasing_id())
        df = df.withColumn("row_num_temp", F.row_number().over(w))
        df = df.withColumn("cd_silver_id", F.col("row_num_temp") + F.lit(current_max_silver_id).cast("long"))
        df = df.withColumn("dt_record_to_silver", F.current_timestamp())

        priority_cols = ["cd_silver_id", "dt_record_to_silver", "cd_bronze_id", "dt_record_to_bronze2"]
        other_cols = [c for c in df.columns if c not in priority_cols and c != "row_num_temp"]
        final_df = df.select(*priority_cols, *other_cols)

        # 5. WRITE TO SILVER (APPEND)
        print(f"Writing to {TARGET_DB}.{TABLE_NAME}...")
        
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {TARGET_DB} LOCATION '{WAREHOUSE_LOCATION}{TARGET_DB}'")
        
        write_opts = (
            final_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .option("path", f"{WAREHOUSE_LOCATION}{TARGET_DB}/{TABLE_NAME}")
        )
        
        if "Date" in final_df.columns:
            write_opts.partitionBy("Date").saveAsTable(f"{TARGET_DB}.{TABLE_NAME}")
        else:
            write_opts.saveAsTable(f"{TARGET_DB}.{TABLE_NAME}")

        print("--- SUCCESS: Data is clean and loaded to Silver ---")

    except Exception as e:
        print(f"--- ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()