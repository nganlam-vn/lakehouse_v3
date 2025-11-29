import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, to_timestamp, date_format, hour, current_timestamp,
    row_number, lit, to_utc_timestamp, substring
)
from delta.tables import DeltaTable

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
MINIO_SSL = False

BUCKET = "warehouse"

BRONZE_DB = "bronze2"
BRONZE_TBL = "stocks_intraday"

SILVER_DB = "silver"
SILVER_TBL = "stocks_intraday"
SILVER_PATH = f"s3a://{BUCKET}/silver/{SILVER_TBL}"

CTRL_DB = "_control"
SILVER_CTRL_TBL = "stocks_intraday_silver_log"
SILVER_CTRL_PATH = f"s3a://{BUCKET}/_control/bronze2_to_silver/{SILVER_CTRL_TBL}"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze2_to_silver_delta")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(MINIO_SSL).lower())
        .config("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
        .getOrCreate()
    )


def ensure_meta(spark: SparkSession):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CTRL_DB}")
    if not DeltaTable.isDeltaTable(spark, SILVER_CTRL_PATH):
        (
            spark.createDataFrame(
                [],
                "run_id STRING, processed_at TIMESTAMP, processed_max_ts STRING"
            )
            .write.format("delta").mode("overwrite").save(SILVER_CTRL_PATH)
        )
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {CTRL_DB}.{SILVER_CTRL_TBL}
            USING DELTA
            LOCATION '{SILVER_CTRL_PATH}'
        """)


def get_max_silver_id(spark: SparkSession) -> int:
    try:
        if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
            return 0
        from pyspark.sql.functions import max as fmax
        m = spark.read.format("delta").load(SILVER_PATH).agg(fmax("cd_silver_id")).collect()[0][0]
        return int(m) if m is not None else 0
    except Exception:
        return 0


def transform_and_clean(df_bronze):
    drop_cols = ["date_ny", "interval", "tz", "_src_path"]
    existing_drop_cols = [c for c in drop_cols if c in df_bronze.columns]
    if existing_drop_cols:
        df_bronze = df_bronze.drop(*existing_drop_cols)

    df_typed = df_bronze.withColumn(
        "timestamp", 
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS")
    )

    df_clean = df_typed.filter(
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

    df_enriched = (
        df_clean
        .withColumn("date", substring(col("timestamp").cast("string"), 1, 10))
        .withColumn("hour", hour(col("timestamp")))
        .withColumn("avg_price", (col("high") + col("low")) / 2)
    )

    df_final = df_enriched.dropDuplicates(["symbol", "timestamp"])
    return df_final


def attach_ids_and_times(spark: SparkSession, df_silver):
    max_id = get_max_silver_id(spark)
    w = Window.orderBy(col("symbol"), col("timestamp"))
    df_out = (
        df_silver
        .withColumn(
            "dt_record_to_silver",
            date_format(
                to_utc_timestamp(current_timestamp(), "UTC"),
                "yyyy-MM-dd'T'HH:mm:ss'Z'"
            )
        )
        .withColumn("cd_silver_id", (row_number().over(w) + lit(max_id)).cast("long"))
    )
    return df_out


def upsert_to_silver(spark: SparkSession, df_silver):
    df_silver.persist()
    cnt = df_silver.count()
    if cnt == 0:
        print("✅ Không có dữ liệu mới để ghi vào Silver.")
        df_silver.unpersist()
        return

    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        (
            df_silver.write.format("delta")
            .partitionBy("date", "symbol")
            .mode("overwrite")
            .save(SILVER_PATH)
        )
    else:
        delta_tbl = DeltaTable.forPath(spark, SILVER_PATH)
        (
            delta_tbl.alias("t")
            .merge(
                df_silver.alias("s"),
                "t.symbol = s.symbol AND t.timestamp = s.timestamp"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    df_silver.unpersist()

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_DB}.{SILVER_TBL}
        USING DELTA
        LOCATION '{SILVER_PATH}'
    """)
    print(f"✅ Upserted {cnt} record(s) into {SILVER_DB}.{SILVER_TBL}.")


def main():
    spark = build_spark()
    ensure_meta(spark)

    df_bronze_raw = spark.read.table(f"{BRONZE_DB}.{BRONZE_TBL}")
    

    df_bronze = df_bronze_raw.withColumn(
        "timestamp_ts", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS")
    )

    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        df_max_silver = (
            spark.read.format("delta").load(SILVER_PATH)
            .groupBy("symbol")
            .agg({"timestamp": "max"})
            .withColumnRenamed("max(timestamp)", "max_ts")
        )
    else:
        df_max_silver = spark.createDataFrame([], "symbol STRING, max_ts TIMESTAMP")

    df_bronze_inc = (
        df_bronze.alias("b")
        .join(df_max_silver.alias("s"), on="symbol", how="left")
        .filter((col("s.max_ts").isNull()) | (col("b.timestamp_ts") > col("s.max_ts")))
        .drop("max_ts")
    )

    df_silver_base = transform_and_clean(
        df_bronze_inc.drop("timestamp").withColumnRenamed("timestamp_ts", "timestamp")
    )

    if "dt_record_to_bronze" in df_bronze_raw.columns:
        df_silver_base = df_silver_base.withColumn(
            "dt_record_to_bronze", col("dt_record_to_bronze").cast("string")
        )

    df_silver = attach_ids_and_times(spark, df_silver_base)

    cols = df_silver.columns
    front_cols = [c for c in ["cd_silver_id", "cd_bronze_id", "dt_record_to_silver"] if c in cols]
    other_cols = [c for c in cols if c not in front_cols]
    df_silver = df_silver.select(*(front_cols + other_cols))

    upsert_to_silver(spark, df_silver)

    try:
        row = spark.sql(f"SELECT MAX(timestamp) AS max_ts FROM {SILVER_DB}.{SILVER_TBL}").first()
        max_ts_after = row.max_ts.strftime('%Y-%m-%d %H:%M:%S') if row and row.max_ts else None
    except Exception:
        max_ts_after = None

    run_log = (
        spark.createDataFrame([(None,)], "x INT")
        .selectExpr("uuid() as run_id")
        .withColumn("processed_at", current_timestamp())
        .withColumn("processed_max_ts", lit(max_ts_after).cast("string"))
    )

    run_log.write.format("delta").mode("append").save(SILVER_CTRL_PATH)
    spark.stop()


if __name__ == "__main__":
    main()