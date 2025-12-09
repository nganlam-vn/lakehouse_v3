# /opt/jobs/coin/coin_bronze2_to_silver.py
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    row_number,
    max as F_max,
    trim,
    concat_ws,
    coalesce,
)
from pyspark.sql.types import StringType, ArrayType, NumericType
from delta.tables import DeltaTable

BUCKET = "warehouse"

BRONZE_DB = "bronze2"
BRONZE_TBL = "coinmarketcap"
BRONZE_PATH = f"s3a://{BUCKET}/bronze2/{BRONZE_TBL}"

SILVER_DB = "silver"
SILVER_TBL = "coinmarketcap"
SILVER_PATH = f"s3a://{BUCKET}/silver/{SILVER_TBL}"

CTRL_DB = "_control"
CTRL_TBL = "coinmarketcap_silver_log"
CTRL_PATH = f"s3a://{BUCKET}/_control/{CTRL_TBL}"


def create_spark_session():
    spark = (
        SparkSession.builder.appName("coinmarketcap_bronze2_to_silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def ensure_db_and_table(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CTRL_DB}")

    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {SILVER_DB}.{SILVER_TBL}
            USING DELTA
            LOCATION '{SILVER_PATH}'
            """
        )

    if DeltaTable.isDeltaTable(spark, CTRL_PATH):
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {CTRL_DB}.{CTRL_TBL}
            USING DELTA
            LOCATION '{CTRL_PATH}'
            """
        )


def get_last_watermark(spark):
    if not DeltaTable.isDeltaTable(spark, CTRL_PATH):
        return None

    df = spark.read.format("delta").load(CTRL_PATH)
    row = (
        df.orderBy(col("run_at").desc())
        .select("dt_max_record_to_bronze")
        .limit(1)
        .collect()
    )
    if not row:
        return None
    return row[0]["dt_max_record_to_bronze"]


def get_silver_start_id(spark):
    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        return 0

    df = spark.read.format("delta").load(SILVER_PATH)
    row = df.agg(F_max("cd_silver_id").alias("max_id")).collect()[0]
    max_id = row["max_id"]
    if max_id is None:
        return 0
    return int(max_id)


def _safe_col(name: str):
    # dùng backtick để xử lý tên cột có dấu '.', '[', ']'...
    return col("`" + name.replace("`", "``") + "`")


def clean_bronze_df(df):
    from pyspark.sql import functions as F

    # 2) Trim các cột string (trừ 'tags')
    for f in df.schema.fields:
        if isinstance(f.dataType, StringType) and f.name not in ["tags"]:
            df = df.withColumn(f.name, trim(_safe_col(f.name)))

    # 3) Xử lý tags
    if "tags" in df.columns:
        if isinstance(df.schema["tags"].dataType, ArrayType):
            df = df.withColumn("tags", concat_ws(",", _safe_col("tags")))
        df = df.withColumn("tags", trim(_safe_col("tags")))

    # 4) Giữ lại bản ghi mới nhất per id
    if "id" in df.columns and "dt_record_to_bronze" in df.columns:
        w = Window.partitionBy("id").orderBy(col("dt_record_to_bronze").desc())
        df = (
            df.withColumn("row_num", row_number().over(w))
            .filter(col("row_num") == 1)
            .drop("row_num")
        )

    return df


def main():
    spark = create_spark_session()
    ensure_db_and_table(spark)

    # Lọc incremental theo dt_record_to_bronze
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        max_dt = (
            spark.sql(
                f"SELECT MAX(dt_record_to_bronze) AS max_dt FROM {SILVER_DB}.{SILVER_TBL}"
            )
            .collect()[0]["max_dt"]
        )

        if max_dt is None:
            bronze_new = spark.table(f"{BRONZE_DB}.{BRONZE_TBL}")
        else:
            bronze_new = spark.table(f"{BRONZE_DB}.{BRONZE_TBL}").filter(
                col("dt_record_to_bronze") > max_dt
            )
    else:
        bronze_new = spark.table(f"{BRONZE_DB}.{BRONZE_TBL}")

    if bronze_new.rdd.isEmpty():
        print("Không có dữ liệu mới để xử lý")
        spark.stop()
        return

    cleaned = clean_bronze_df(bronze_new)

    start_id = get_silver_start_id(spark)
    w = Window.orderBy(col("cd_bronze_id"), col("dt_record_to_bronze"))

    staged = (
        cleaned.withColumn("rn", row_number().over(w))
        .withColumn("cd_silver_id", col("rn") + lit(start_id))
        .withColumn("dt_record_to_silver", current_timestamp())
        .withColumn("dt_partition", col("dt_record_to_silver").cast("date"))
        .drop("rn")
    )

    # 👉 Reorder cột: dùng _safe_col để tránh lỗi INVALID_EXTRACT_BASE_FIELD_TYPE
    cols = staged.columns
    front_cols = ["cd_silver_id", "cd_bronze_id", "dt_record_to_silver"]
    other_cols = [c for c in cols if c not in front_cols]

    front_exprs = [_safe_col(c) for c in front_cols]
    other_exprs = [_safe_col(c) for c in other_cols]
    staged = staged.select(*(front_exprs + other_exprs))

    # Ghi dữ liệu
    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        (
            staged.write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .partitionBy("dt_partition")
            .save(SILVER_PATH)
        )
    else:
        (
            staged.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("dt_partition")
            .save(SILVER_PATH)
        )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_DB}.{SILVER_TBL}
        USING DELTA
        LOCATION '{SILVER_PATH}'
    """
    )

    print(">>> Hoàn thành bronze2 → silver thành công!")
    spark.stop()


if __name__ == "__main__":
    main()
