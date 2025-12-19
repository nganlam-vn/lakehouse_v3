#bronze2_to_silver.py
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, to_timestamp, date_format, hour, current_timestamp,
    row_number, lit, to_utc_timestamp, max as spark_max, to_date
)
from delta.tables import DeltaTable

# === CẤU HÌNH ===
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
MINIO_SSL = False

BUCKET = "warehouse"
BRONZE_PATH = f"s3a://{BUCKET}/bronze2/stocks_intraday"
SILVER_PATH = f"s3a://{BUCKET}/silver/stocks_intraday"
SILVER_DB = "silver"
SILVER_TBL = "stocks_intraday"

CHECKPOINT_PATH = f"s3a://{BUCKET}/_checkpoints/bronze2_to_silver"

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze2_to_silver_streaming_timestamp_incremental")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(MINIO_SSL).lower())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

def get_max_silver_timestamp(spark) -> str:
    """
    Truy vấn MAX(timestamp) từ bảng Silver.
    Trả về chuỗi timestamp hoặc None nếu bảng chưa tồn tại/rỗng.
    """
    try:
        if DeltaTable.isDeltaTable(spark, SILVER_PATH):
            # Đọc bảng Silver để lấy max timestamp
            max_ts_row = spark.read.format("delta").load(SILVER_PATH).agg(spark_max("timestamp")).collect()[0][0]
            if max_ts_row:
                return max_ts_row
        return None
    except Exception as e:
        print(f"⚠️ Warning getting max timestamp: {e}")
        return None

def get_next_silver_id(spark) -> int:
    """Lấy Max ID hiện tại trong bảng Silver để tạo ID tiếp theo."""
    try:
        if DeltaTable.isDeltaTable(spark, SILVER_PATH):
            max_id_row = spark.read.format("delta").load(SILVER_PATH).agg(spark_max("cd_silver_id")).collect()[0][0]
            return int(max_id_row) if max_id_row is not None else 0
        return 0
    except Exception:
        return 0

def process_batch(batch_df, batch_id):

    spark = batch_df.sparkSession
    
    if batch_df.rdd.isEmpty():
        return

    # 1. Lấy MAX Timestamp từ Silver (Watermark)
    max_silver_ts = get_max_silver_timestamp(spark)
    print(f"🔎 Max Silver Timestamp found: {max_silver_ts}")

    # 2. Clean & Convert Type
    df_typed = batch_df.withColumn(
        "timestamp", 
        to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
    )

    # 3. Lọc dữ liệu MỚI HƠN max timestamp (Incremental Logic)
    if max_silver_ts:
        df_filtered = df_typed.filter(col("timestamp") > lit(max_silver_ts))
        print(f"   📉 Filtering: Keeping rows with timestamp > {max_silver_ts}")
    else:
        # Nếu chưa có dữ liệu trong Silver (Lần chạy đầu), lấy tất cả
        df_filtered = df_typed
        print("No existing Silver data. Processing all rows in batch.")

    if df_filtered.rdd.isEmpty():
        print(" No new data after filtering. Skipping batch.")
        return

    # 4. Áp dụng các bộ lọc nghiệp vụ (Business Rules)
    df_clean = df_filtered.filter(
        col("timestamp").isNotNull() &
        col("symbol").isNotNull() &
        col("open").isNotNull() & (col("open") > 0) &
        col("high").isNotNull() & (col("high") > 0) &
        col("low").isNotNull() & (col("low") > 0) &
        col("close").isNotNull() & (col("close") > 0) &
        col("volume").isNotNull() & (col("volume") >= 0) &
        (col("low") <= col("high")) &
        (col("low") <= col("open")) & (col("low") <= col("close")) &
        (col("high") >= col("open")) & (col("high") >= col("close"))
    )

    # Deduplicate trong batch hiện tại
    df_dedup = df_clean.dropDuplicates(["symbol", "timestamp"])

    if df_dedup.rdd.isEmpty():
        return

    # Feature Engineering
    df_enriched = (
        df_dedup
        .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd")) # Cột Partition
        .withColumn("hour", hour(col("timestamp")))
        .withColumn("avg_price", (col("high") + col("low")) / 2)
    )

    # 5. Tạo ID Silver & Metadata thời gian
    current_max_id = get_next_silver_id(spark)
    w = Window.orderBy("timestamp", "symbol")

    df_final = (
        df_enriched
        .withColumn("row_num", row_number().over(w))
        .withColumn("cd_silver_id", (col("row_num") + lit(current_max_id)).cast("long"))
        .withColumn("dt_record_to_silver", date_format(to_utc_timestamp(current_timestamp(), "UTC"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .drop("row_num")
    )

    # Sắp xếp cột
    cols = df_final.columns
    priority_cols = ["cd_silver_id", "cd_bronze_id", "symbol", "timestamp", "date"]
    other_cols = [c for c in cols if c not in priority_cols]
    df_final = df_final.select(*(priority_cols + other_cols))

    # 6. Ghi vào Silver (Append hoặc Upsert)
    
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        print(f"Merging {df_final.count()} rows into Silver...")
        delta_tbl = DeltaTable.forPath(spark, SILVER_PATH)
        (
            delta_tbl.alias("t")
            .merge(
                df_final.alias("s"),
                "t.symbol = s.symbol AND t.timestamp = s.timestamp"
            )
            .whenNotMatchedInsertAll() # Chỉ cần Insert vì ta đã filter > max rồi
            .execute()
        )
    else:
        print(f"Creating new Silver table with {df_final.count()} rows...")
        (
            df_final.write
            .format("delta")
            .partitionBy("date", "symbol")
            .mode("overwrite")
            .save(SILVER_PATH)
        )
        
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {SILVER_DB}.{SILVER_TBL}
            USING DELTA
            LOCATION '{SILVER_PATH}'
        """)

def main():
    spark = build_spark()
    
    if not DeltaTable.isDeltaTable(spark, BRONZE_PATH):
        print("Bronze table not found via Delta. Please ensure Bronze 2 is created.")
        return

    print(f"🚀 Processing Silver Layer from {BRONZE_PATH}...")

    # Đọc Streaming từ Bronze 2
    df_stream = (
        spark.readStream
        .format("delta")
        .option("ignoreChanges", "true") 
        .load(BRONZE_PATH)
    )

    query = (
        df_stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    print("✅ Silver Layer Updated Successfully!")
    spark.stop()

if __name__ == "__main__":
    main()