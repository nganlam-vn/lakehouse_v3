#silver_to_gold.py
import os
import sys
import traceback
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- CẤU HÌNH ---
SOURCE_DB = "silver"
TARGET_DB = "gold"
TABLE_NAME = os.getenv("TABLE_NAME", "uber_bookings")

WAREHOUSE_LOCATION = "s3a://warehouse/"
METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")

if __name__ == "__main__":
    print(f"--- Starting Gold Job: {SOURCE_DB} -> {TARGET_DB} ---")

    spark = (
        SparkSession.builder
        .appName("silver_to_gold_etl")
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
        # Đọc Silver
        print(f"Reading from {SOURCE_DB}.{TABLE_NAME}...")
        df = spark.read.table(f"{SOURCE_DB}.{TABLE_NAME}")

        # Tạo Database Gold
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {TARGET_DB} LOCATION '{WAREHOUSE_LOCATION}{TARGET_DB}'")

        # 1. BẢNG DAILY KPI
        print("--- 1. Aggregating Daily KPI ---")
        df_completed = df.withColumn("Real_Revenue", 
                                     F.when(F.col("Booking_Status") == "Completed", F.col("Booking_Value"))
                                      .otherwise(0))
        
        daily_kpi = df_completed.groupBy("Date", "Vehicle_Type").agg(
            F.sum("Real_Revenue").alias("Total_Revenue"),
            F.count("*").alias("Total_Rides"),
            F.sum(F.when(F.col("Booking_Status") == "Completed", 1).otherwise(0)).alias("Completed_Rides"),
            F.sum(F.when(F.col("Booking_Status") != "Completed", 1).otherwise(0)).alias("Cancelled_Rides"),
            F.round(F.avg("Avg_VTAT"), 2).alias("Avg_Wait_Time")
        )
        daily_kpi = daily_kpi.withColumn("Cancellation_Rate", 
                                         F.round((F.col("Cancelled_Rides") / F.col("Total_Rides")) * 100, 2))

        print(f"Writing {TARGET_DB}.daily_kpi_vehicle...")
        daily_kpi.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{TARGET_DB}.daily_kpi_vehicle")

        # 2. BẢNG RUSH HOUR ANALYSIS
        print("--- 2. Aggregating Rush Hour Analysis ---")
        rush_hour_stats = df.groupBy("Date", "TimeZone", "RushHour").agg(
            F.count("*").alias("Total_Bookings"),
            F.round(F.avg("Booking_Value"), 2).alias("Avg_Booking_Value"),
            F.round(F.avg("Avg_VTAT"), 2).alias("Avg_Wait_Time_Mins"),
            F.round(F.sum("Booking_Value") / F.sum("Ride_Distance"), 2).alias("Revenue_Per_Km")
        )
        print(f"Writing {TARGET_DB}.rush_hour_stats...")
        rush_hour_stats.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("Date").saveAsTable(f"{TARGET_DB}.rush_hour_stats")

        # 3. BẢNG CANCELLATION REASONS
        print("--- 3. Aggregating Cancellation Reasons ---")
        df_cancel = df.filter(F.col("Booking_Status") != "Completed")
        df_cancel = df_cancel.withColumn("Month", F.date_format("Date", "yyyy-MM"))
        
        df_normalized = df_cancel.withColumn("Cancellation_Type",
            F.when(F.col("Booking_Status").like("%Customer%"), "Customer")
             .when(F.col("Booking_Status").like("%Driver%"), "Driver")
             .when(F.col("Booking_Status") == "Incomplete", "Incomplete")
             .otherwise("Other")
        ).withColumn("Reason",
            F.when(F.col("Booking_Status").like("%Customer%"), F.col("Reason_for_cancelling_by_Customer"))
             .when(F.col("Booking_Status").like("%Driver%"), F.col("Driver_Cancellation_Reason"))
             .when(F.col("Booking_Status") == "Incomplete", F.col("Incomplete_Rides_Reason"))
             .otherwise("Unknown")
        )

        cancellation_reasons = df_normalized.groupBy("Month", "Cancellation_Type", "Reason").agg(
            F.count("*").alias("Total_Cancellations"),
            F.round(F.sum("Booking_Value"), 2).alias("Estimated_Lost_Revenue")
        )
        print(f"Writing {TARGET_DB}.cancellation_reasons...")
        cancellation_reasons.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{TARGET_DB}.cancellation_reasons")

        # 4. BẢNG PAYMENT METHOD STATS 
        print("--- 4. Aggregating Payment Method Stats ---")
        
        # Chỉ tính Payment cho các chuyến Completed (Vì chuyến hủy ko trả tiền)
        df_pay = df.filter(F.col("Booking_Status") == "Completed")
        
        # Thống kê theo Tháng và Phương thức
        payment_stats = df_pay.groupBy(F.date_format("Date", "yyyy-MM").alias("Month"), "Payment_Method").agg(
            F.count("*").alias("Total_Transactions"),
            F.round(F.sum("Booking_Value"), 2).alias("Total_Revenue"),
            F.round(F.avg("Booking_Value"), 2).alias("Avg_Transaction_Value")
        )
        
        print(f"Writing {TARGET_DB}.payment_method_stats...")
        payment_stats.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(f"{TARGET_DB}.payment_method_stats")
            
        print("-> Saved gold.payment_method_stats")

        print("--- SUCCESS: Gold Layer Processing Complete ---")

    except Exception as e:
        print(f"--- ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()