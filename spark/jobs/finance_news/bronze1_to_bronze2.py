import os, sys
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import pyspark.sql.functions as F


# ==== CONFIG ====
BUCKET   = os.getenv("BUCKET", "warehouse")
DB_NAME  = os.getenv("DB_NAME", "bronze2")
TBL_NAME = os.getenv("TBL_NAME", "finance_news")

SRC = f"s3a://{BUCKET}/bronze1/finance_news"
DST = f"s3a://{BUCKET}/bronze2/finance_news"

CKPT_DB  = "ckpt"
CKPT_TBL = "finance_news_b1b2"
CKPT_DIR = f"s3a://{BUCKET}/_checkpoints/bronze1_to_bronze2/finance_news"


def build_spark():
    return (
        SparkSession.builder
        .appName("finance_news_bronze1_to_bronze2_sql")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def main():
    spark = build_spark()

    df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .option("multiLine", "true")
        .json(SRC)
    ).withColumn("_src_path", F.input_file_name())

    if df.limit(1).count() == 0:
        print("No input → exit.")
        spark.stop()
        return

    df.createOrReplaceTempView("bronze1_raw")

    if not DeltaTable.isDeltaTable(spark, CKPT_DIR):
        (
            spark.createDataFrame([], "src_path string, processed_ts timestamp")
            .write.format("delta").mode("overwrite").save(CKPT_DIR)
        )
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CKPT_DB}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {CKPT_DB}.{CKPT_TBL} USING DELTA LOCATION '{CKPT_DIR}'"
    )
    spark.read.format("delta").load(CKPT_DIR).createOrReplaceTempView("ckpt")

    new_df = spark.sql("""
        SELECT *
        FROM bronze1_raw b
        LEFT ANTI JOIN ckpt c
        ON b._src_path = c.src_path
    """)

    if new_df.limit(1).count() == 0:
        print("No new files → exit.")
        spark.stop()
        return

    new_df.createOrReplaceTempView("bronze1_new")

    if DeltaTable.isDeltaTable(spark, DST):
        spark.read.format("delta").load(DST).createOrReplaceTempView("bronze2_old")
        max_id = spark.sql("SELECT COALESCE(MAX(cd_bronze_id), 0) AS mx FROM bronze2_old").first().mx
    else:
        max_id = 0

    spark.sql(f"SET start_id = {max_id}")

    transformed = spark.sql("""
        SELECT
            -- đưa 2 cột lên đầu
            (ROW_NUMBER() OVER (ORDER BY _src_path) + ${start_id}) AS cd_bronze_id,
            DATE_FORMAT(CURRENT_TIMESTAMP(), "yyyy-MM-dd'T'HH:mm:ss'Z'") AS dt_record_to_bronze,

            -- giữ nguyên mọi cột JSON
            b.* EXCEPT (_src_path),

            -- partition date
            TO_DATE(CURRENT_TIMESTAMP()) AS date
        FROM bronze1_new b
    """)

    transformed.createOrReplaceTempView("bronze2_final")

    (
        transformed.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("date")
        .save(DST)
    )

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DB_NAME}.{TBL_NAME}
        USING DELTA
        LOCATION '{DST}'
    """)

    spark.sql("""
        SELECT DISTINCT _src_path AS src_path, CURRENT_TIMESTAMP() AS processed_ts
        FROM bronze1_new
    """).write.format("delta").mode("append").save(CKPT_DIR)

    print("🎉 Done using Spark SQL!")
    spark.stop()


if __name__ == "__main__":
    main()
