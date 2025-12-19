import os
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# ==== CONFIG ĐƯỜNG DẪN / BIẾN MÔI TRƯỜNG ====
BUCKET          = os.getenv("BUCKET", "warehouse")

BRONZE1_PREFIX  = os.getenv("BRONZE1_PREFIX", "bronze1/finance_news")
BRONZE2_PREFIX  = os.getenv("BRONZE2_PREFIX", "bronze2/finance_news")

SRC_PATH        = os.getenv("SRC_PATH", f"s3a://{BUCKET}/{BRONZE1_PREFIX}")
DST_PATH        = os.getenv("DST_PATH", f"s3a://{BUCKET}/{BRONZE2_PREFIX}")

DST_DB          = os.getenv("DST_DB", "bronze2")
DST_TBL         = os.getenv("DST_TBL", "finance_news")

CKPT_DB         = os.getenv("CKPT_DB", "ckpt")
CKPT_TBL        = os.getenv("CKPT_TBL", "finance_news_b1b2")
CKPT_DIR        = os.getenv(
    "CKPT_DIR",
    f"s3a://{BUCKET}/_checkpoints/bronze1_to_bronze2/finance_news"
)


def build_spark():
    return (
        SparkSession.builder
        .appName("finance_news_bronze1_to_bronze2_dfapi")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def ensure_ckpt_table(spark: SparkSession):
    if not DeltaTable.isDeltaTable(spark, CKPT_DIR):
        (
            spark.createDataFrame([], "src_path string, processed_ts timestamp")
            .write.format("delta").mode("overwrite").save(CKPT_DIR)
        )

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CKPT_DB}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {CKPT_DB}.{CKPT_TBL} "
        f"USING DELTA LOCATION '{CKPT_DIR}'"
    )

    return spark.read.format("delta").load(CKPT_DIR)


def get_new_files(df_all, df_ckpt):
    df_all = df_all.withColumn("_src_path", F.input_file_name())
    df_ckpt = df_ckpt.select("src_path").dropDuplicates()

    df_new = df_all.join(
        df_ckpt,
        df_all["_src_path"] == df_ckpt["src_path"],
        "left_anti"
    )
    return df_new


def get_max_bronze_id(spark: SparkSession):
    if DeltaTable.isDeltaTable(spark, DST_PATH):
        df_old = spark.read.format("delta").load(DST_PATH)
        if "cd_bronze_id" in df_old.columns:
            row = df_old.agg(F.max("cd_bronze_id").alias("mx")).first()
            return int(row.mx) if row.mx is not None else 0
    return 0


def transform_to_bronze2(df_new, start_id: int):
    if df_new.rdd.isEmpty():
        return df_new

    w = Window.orderBy("_src_path")

    df = (
        df_new
        .withColumn(
            "cd_bronze_id",
            (F.row_number().over(w) + F.lit(start_id)).cast("long")
        )
        .withColumn(
            "dt_record_to_bronze",
            F.date_format(
                F.current_timestamp(),
                "yyyy-MM-dd'T'HH:mm:ss'Z'"
            )
        )
    )

    # Nếu có cột partition date từ folder thì cast sang DATE,
    # nếu không thì dùng current_date()
    if "date" in df.columns:
        df = df.withColumn("date", F.to_date("date"))
    else:
        df = df.withColumn("date", F.current_date())

    # Đảm bảo luôn có cột symbol (nếu dữ liệu cũ chưa có)
    if "symbol" not in df.columns:
        df = df.withColumn("symbol", F.lit(None).cast("string"))

    df = df.drop("_src_path")
    return df


def write_bronze2(df_tr, spark: SparkSession):
    (
        df_tr.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("date")
        .save(DST_PATH)
    )

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DST_DB}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DST_DB}.{DST_TBL}
        USING DELTA
        LOCATION '{DST_PATH}'
    """)


def update_ckpt(df_new):
    df_ckpt_append = (
        df_new
        .select("_src_path")
        .dropDuplicates()
        .withColumn("processed_ts", F.current_timestamp())
        .withColumnRenamed("_src_path", "src_path")
    )

    (
        df_ckpt_append.write
        .format("delta")
        .mode("append")
        .save(CKPT_DIR)
    )


def main():
    spark = build_spark()

    df_all = (
        spark.read
        .option("multiLine", "true")
        .json(SRC_PATH)
    )

    if df_all.rdd.isEmpty():
        print("No input → exit.")
        spark.stop()
        return

    df_ckpt = ensure_ckpt_table(spark)
    df_new = get_new_files(df_all, df_ckpt)

    if df_new.rdd.isEmpty():
        print("No new files → exit.")
        spark.stop()
        return

    max_id = get_max_bronze_id(spark)
    print(f"Current max cd_bronze_id = {max_id}")

    df_tr = transform_to_bronze2(df_new, max_id)

    if df_tr.rdd.isEmpty():
        print("Nothing to write after transform → exit.")
        spark.stop()
        return

    write_bronze2(df_tr, spark)
    update_ckpt(df_new)

    print("🎉 Done bronze1 → bronze2 (finance_news)!")
    spark.stop()


if __name__ == "__main__":
    main()
