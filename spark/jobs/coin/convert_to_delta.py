from datetime import datetime
from typing import Optional
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable


BUCKET = os.getenv("BUCKET", "warehouse")

BRONZE1_PREFIX = "bronze1/coinmarketcap"

DST_DB = "bronze2"
DST_TBL = "coinmarketcap"
DST_PATH = f"s3a://{BUCKET}/bronze2/{DST_TBL}"

CTRL_DB = "_control"
CTRL_TBL = "coinmarketcap_ingest_log"
CTRL_PATH = f"s3a://{BUCKET}/_control/{CTRL_TBL}"


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("coinmarketcap_bronze1_to_bronze2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def get_last_key(spark: SparkSession) -> Optional[str]:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CTRL_DB}")

    try:
        df = spark.read.format("delta").load(CTRL_PATH)

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {CTRL_DB}.{CTRL_TBL}
            USING DELTA
            LOCATION '{CTRL_PATH}'
            """
        )

        row = df.orderBy("run_at", ascending=False).limit(1).collect()
        if row:
            return row[0]["last_key"]

        return None

    except Exception:
        return None


def list_new_objects(spark, last_key):
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()

    fs_class = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    path_class = sc._gateway.jvm.org.apache.hadoop.fs.Path

    s3a_path = path_class(f"s3a://{BUCKET}/{BRONZE1_PREFIX}/")
    fs = fs_class.get(s3a_path.toUri(), hadoop_conf)

    new_keys = []

    if fs.exists(s3a_path):
        it = fs.listFiles(s3a_path, True)
        while it.hasNext():
            file = it.next()
            key = file.getPath().toString().replace(f"s3a://{BUCKET}/", "")

            if key.endswith(".json") and (last_key is None or key > last_key):
                new_keys.append(key)

    return sorted(new_keys)


def update_checkpoint(spark: SparkSession, last_key: str):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CTRL_DB}")

    log_df = (
        spark.createDataFrame([(last_key,)], ["last_key"])
        .withColumn("run_at", F.current_timestamp())
    )

    log_df.write.format("delta").mode("append").save(CTRL_PATH)

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {CTRL_DB}.{CTRL_TBL}
        USING DELTA
        LOCATION '{CTRL_PATH}'
        """
    )


def get_max_bronze_id(spark: SparkSession) -> int:
    try:
        if not DeltaTable.isDeltaTable(spark, DST_PATH):
            return 0

        df = spark.read.format("delta").load(DST_PATH)
        row = df.agg(F.max("cd_bronze_id").alias("maxid")).collect()[0]

        return int(row["maxid"]) if row["maxid"] else 0

    except:
        return 0


def main():
    spark = create_spark_session()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DST_DB}")

    last_key = get_last_key(spark)
    print(f"[INFO] last_key = {last_key}")

    new_keys = list_new_objects(spark, last_key)
    if not new_keys:
        print("[INFO] Không có file mới → exit")
        return

    print(f"[INFO] Found {len(new_keys)} new files")

    paths = [f"s3a://{BUCKET}/{k}" for k in new_keys]

    df = spark.read.json(paths)

    df = df.withColumn("dt_record_to_bronze", F.current_timestamp())
    df = df.withColumn("dt_record_to_bronze_date", F.to_date("dt_record_to_bronze"))

    start_id = get_max_bronze_id(spark)
    w = Window.orderBy(F.monotonically_increasing_id())

    df = df.withColumn("cd_bronze_id", F.row_number().over(w) + F.lit(start_id))

    cols = df.columns
    front_cols = ["cd_bronze_id", "dt_record_to_bronze"]
    other_cols = [c for c in cols if c not in front_cols]

    front_exprs = [F.col(c) for c in front_cols]
    other_exprs = []
    for c in other_cols:
        if "." in c or "[" in c or "]" in c:
            other_exprs.append(F.col(f"`{c}`"))
        else:
            other_exprs.append(F.col(c))

    df = df.select(*(front_exprs + other_exprs))

    (
        df.write.format("delta")
        .partitionBy("dt_record_to_bronze_date")
        .mode("append")
        .option("mergeSchema", "true")
        .save(DST_PATH)
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {DST_DB}.{DST_TBL}
        USING DELTA
        LOCATION '{DST_PATH}'
        """
    )

    update_checkpoint(spark, max(new_keys))

    spark.stop()
    print("[SUCCESS] Bronze1 → Bronze2 done!")


if __name__ == "__main__":
    main()
