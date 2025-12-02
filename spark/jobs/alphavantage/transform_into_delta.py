import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    current_timestamp, col, row_number, lit,
    date_format, to_utc_timestamp, to_timestamp, substring
)
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
MINIO_SSL = False

BUCKET = "warehouse"
SRC_PREFIX = "bronze1/stocks_intraday"
DST_PREFIX = "bronze2/stocks_intraday"

SRC_PATH = f"s3a://{BUCKET}/{SRC_PREFIX}/"
DST_PATH = f"s3a://{BUCKET}/{DST_PREFIX}"

DB_NAME = "bronze2"
TBL_NAME = "stocks_intraday"

CTRL_DB = "_control"
CTRL_TBL = "stocks_intraday_ingest_log"
CTRL_PATH = f"s3a://{BUCKET}/_control/bronze1_to_bronze2/{CTRL_TBL}"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze1_to_bronze2_json_to_delta_checkpointed")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(MINIO_SSL).lower())
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .getOrCreate()
    )


def ensure_tables(spark: SparkSession):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CTRL_DB}")
    if not DeltaTable.isDeltaTable(spark, CTRL_PATH):
        (
            spark.createDataFrame(
                [],
                "path STRING, length LONG, modificationTime TIMESTAMP, processed_at TIMESTAMP"
            )
            .write.format("delta").mode("overwrite").save(CTRL_PATH)
        )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {CTRL_DB}.{CTRL_TBL}
        USING DELTA
        LOCATION '{CTRL_PATH}'
        """
    )


def list_source_files(spark: SparkSession):
    return (
        spark.read.format("binaryFile")
        .option("recursiveFileLookup", "true")
        .load(SRC_PATH)
        .selectExpr("path", "length", "CAST(modificationTime AS TIMESTAMP) AS modificationTime")
    )


def read_ingest_log(spark: SparkSession):
    return spark.read.table(f"{CTRL_DB}.{CTRL_TBL}").select("path")


def get_max_bronze_id(spark: SparkSession) -> int:
    try:
        if not DeltaTable.isDeltaTable(spark, DST_PATH):
            return 0
        df_existing = spark.read.format("delta").load(DST_PATH)
        if "cd_bronze_id" not in df_existing.columns:
            return 0
        from pyspark.sql.functions import max as fmax
        row = df_existing.agg(fmax("cd_bronze_id").alias("max_id")).collect()[0]
        m = row["max_id"]
        return int(m) if m is not None else 0
    except AnalysisException:
        return 0
    except Exception:
        return 0


def main():
    spark = build_spark()
    ensure_tables(spark)

    files_df = list_source_files(spark)
    log_df = read_ingest_log(spark)
    new_files_df = files_df.join(log_df, on="path", how="left_anti")

    n = new_files_df.count()
    print(f"Found {n} new file(s) in {SRC_PATH}")
    if n == 0:
        print("No new files. Nothing to ingest.")
        spark.stop()
        return

    new_paths = [r["path"] for r in new_files_df.select("path").collect()]

    df = spark.read.option("multiLine", "false").json(new_paths)

    drop_cols = ["date_ny", "interval", "tz", "_src_path"]
    existing_drop_cols = [c for c in drop_cols if c in df.columns]
    if existing_drop_cols:
        df = df.drop(*existing_drop_cols)

    max_id = get_max_bronze_id(spark)
    w = Window.orderBy(col("symbol"), col("timestamp"))

    df_out = (
        df
        .withColumn(
            "dt_record_to_bronze",
            date_format(
                to_utc_timestamp(current_timestamp(), "UTC"),
                "yyyy-MM-dd'T'HH:mm:ss'Z'"
            )
        )
        .withColumn(
            "date",
            substring(col("timestamp"), 1, 10)  # ✅ Simple: extract "2025-11-28"
        )
        .withColumn("cd_bronze_id", (row_number().over(w) + lit(max_id)).cast("long"))
    )

    cols = df_out.columns
    front_cols = ["cd_bronze_id", "dt_record_to_bronze"]
    other_cols = [c for c in cols if c not in front_cols]
    df_out = df_out.select(*(front_cols + other_cols))

    writer = (
        df_out.write
        .format("delta")
        .partitionBy("date")
        .option("mergeSchema", "true")
    )

    if DeltaTable.isDeltaTable(spark, DST_PATH):
        writer = writer.mode("append")
    else:
        writer = writer.mode("overwrite")

    writer.save(DST_PATH)

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {DB_NAME}.{TBL_NAME}
        USING DELTA
        LOCATION '{DST_PATH}'
        """
    )

    to_log = new_files_df.withColumn("processed_at", current_timestamp())
    to_log.write.format("delta").mode("append").save(CTRL_PATH)

    print(f"Ingested {n} file(s) → {DST_PATH}")
    print(f"Updated checkpoint at {CTRL_DB}.{CTRL_TBL}")
    spark.stop()


if __name__ == "__main__":
    main()
