# -*- coding: utf-8 -*-
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, sha2, to_timestamp, to_date, current_timestamp, current_date,
    when, trim, regexp_replace, row_number,
    lower, date_sub, to_utc_timestamp, date_format
)
from delta.tables import DeltaTable

# ==== CONFIG MINIO / DELTA ====
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() in ("1","true","yes","y")

BUCKET          = os.getenv("BUCKET", "warehouse")
BRONZE2_PREFIX  = os.getenv("BRONZE2_PREFIX", "bronze2/finance_news")
SILVER_PREFIX   = os.getenv("SILVER_PREFIX", "silver/finance_news")

SRC_PATH        = os.getenv("SRC_PATH", f"s3a://{BUCKET}/{BRONZE2_PREFIX}")
DST_PATH        = os.getenv("DST_PATH", f"s3a://{BUCKET}/{SILVER_PREFIX}")
DST_DB          = os.getenv("DST_DB", "silver")
DST_TBL         = os.getenv("DST_TBL", "finance_news")

DAYS_BACK       = int(os.getenv("DAYS_BACK", "0"))

HIVE_METASTORE_URIS = os.getenv("HIVE_METASTORE_URIS", "thrift://hive-metastore:9083")
WAREHOUSE_DIR       = os.getenv("SPARK_SQL_WAREHOUSE_DIR", f"s3a://{BUCKET}/_spark_warehouse")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze2_to_silver__finance_news")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(MINIO_SECURE).lower())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", HIVE_METASTORE_URIS)
        .config("spark.hadoop.hive.metastore.uris", HIVE_METASTORE_URIS)
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .getOrCreate()
    )


def read_bronze2(spark: SparkSession):
    df = spark.read.format("delta").load(SRC_PATH)

    if "date" in df.columns and DAYS_BACK > 0:
        lower_bound = date_sub(current_date(), DAYS_BACK)
        upper_bound = current_date()
        df = df.filter((col("date") >= lower_bound) & (col("date") < upper_bound))

    return df


def normalize(df):
    x = (
        df
          .withColumn("cd_bronze_id", col("cd_bronze_id").cast("long"))
          .withColumn("dt_record_to_bronze", col("dt_record_to_bronze").cast("string"))
          .withColumn("source_name", col("source.name").cast("string"))
          .withColumn("author",       trim(regexp_replace(col("author").cast("string"), r"[\\r\\n]+", " ")))
          .withColumn("title",        trim(regexp_replace(col("title").cast("string"), r"[\\r\\n]+", " ")))
          .withColumn("description",  trim(regexp_replace(col("description").cast("string"), r"[\\r\\n]+", " ")))
          .withColumn("url",          trim(col("url").cast("string")))
          .withColumn("url_to_image", trim(col("urlToImage").cast("string")))
          .withColumn("content",      col("content").cast("string"))
          .withColumn("published_at", to_timestamp(col("publishedAt").cast("string")))
    )

    bronze_ts = to_timestamp(col("dt_record_to_bronze"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    x = x.withColumn(
        "event_date",
        when(col("published_at").isNotNull(), to_date(col("published_at")))
        .otherwise(to_date(bronze_ts))
    )

    x = x.filter(col("url").isNotNull() & col("event_date").isNotNull())
    x = x.withColumn("article_id", sha2(lower(col("url")), 256))

    w = Window.partitionBy("url").orderBy(col("published_at").desc_nulls_last(), bronze_ts.desc_nulls_last())
    x = x.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")
    x = x.dropDuplicates(["article_id"])

    return x


def register_table_in_metastore(spark: SparkSession):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DST_DB}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {DST_DB}.{DST_TBL} USING DELTA LOCATION '{DST_PATH}'")
    spark.sql(f"ALTER TABLE {DST_DB}.{DST_TBL} SET TBLPROPERTIES ('delta.feature.allowColumnDefaults'='supported')")
    spark.catalog.refreshTable(f"{DST_DB}.{DST_TBL}")


def ensure_table_storage(spark: SparkSession, sample_df):
    from pyspark.sql.functions import lit

    if not DeltaTable.isDeltaTable(spark, DST_PATH):
        empty = (
            sample_df
            .limit(0)
            .withColumn("cd_silver_id", lit(None).cast("long"))
            .withColumn("dt_record_to_silver", lit(None).cast("string"))
        )

        cols = empty.columns
        front_cols = ["cd_silver_id", "cd_bronze_id", "dt_record_to_silver"]
        other_cols = [c for c in cols if c not in front_cols]

        empty = empty.select(*(front_cols + other_cols))

        (
            empty.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .partitionBy("event_date")
                .save(DST_PATH)
        )

    register_table_in_metastore(spark)
    return DeltaTable.forPath(spark, DST_PATH)


def apply_day_window(df):
    if DAYS_BACK <= 0:
        return df

    lower_bound = date_sub(current_date(), DAYS_BACK)
    upper_bound = current_date()

    if "date" in df.columns:
        return df.filter((col("date") >= lower_bound) & (col("date") < upper_bound))
    else:
        return df.filter((col("event_date") >= lower_bound) & (col("event_date") < upper_bound))


def prepare_source_with_ids(spark: SparkSession, delta_tbl, df_norm):
    from pyspark.sql.functions import max as fmax

    # Lấy các id đang có trong silver
    existing = (
        delta_tbl.toDF()
        .select("article_id", "cd_silver_id", "dt_record_to_silver")
        .distinct()
    )

    # Join để biết bản ghi nào đã tồn tại
    df_joined = df_norm.join(existing, on="article_id", how="left")

    # Lấy max id hiện tại
    try:
        m = delta_tbl.toDF().agg(fmax("cd_silver_id")).collect()[0][0]
        max_id = int(m) if m is not None else 0
    except Exception:
        max_id = 0

    w = Window.orderBy(col("article_id"))

    # Nếu chưa có cd_silver_id thì cấp id mới + timestamp mới
    df_with_ids = (
        df_joined
        .withColumn(
            "cd_silver_id",
            when(col("cd_silver_id").isNull(),
                 (row_number().over(w) + lit(max_id)).cast("long")
            ).otherwise(col("cd_silver_id"))
        )
        .withColumn(
            "dt_record_to_silver",
            when(
                col("dt_record_to_silver").isNull(),
                date_format(
                    to_utc_timestamp(current_timestamp(), "UTC"),
                    "yyyy-MM-dd'T'HH:mm:ss'Z'"
                )
            ).otherwise(col("dt_record_to_silver"))
        )
    )

    return df_with_ids



def upsert(delta_tbl, df_merge_ready):
    (
        delta_tbl.alias("t")
        .merge(df_merge_ready.alias("s"), "t.article_id = s.article_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def main():
    spark = build_spark()
    print(f"SRC_PATH = {SRC_PATH}")
    print(f"DST_PATH = {DST_PATH}  ({DST_DB}.{DST_TBL})")

    src = read_bronze2(spark)
    if src.rdd.isEmpty():
        print("No rows in bronze2. Exit 0.")
        spark.stop()
        return

    df = normalize(src)
    if df.rdd.isEmpty():
        print("Nothing to upsert after normalization. Exit 0.")
        spark.stop()
        return

    df = apply_day_window(df)
    if df.rdd.isEmpty():
        print("Nothing after day-window filter. Exit 0.")
        spark.stop()
        return

    silver_tbl = ensure_table_storage(spark, df.limit(1))
    src_for_merge = prepare_source_with_ids(spark, silver_tbl, df)

    cols = src_for_merge.columns
    front_cols = [c for c in ["cd_silver_id", "cd_bronze_id", "dt_record_to_silver"] if c in cols]
    other_cols = [c for c in cols if c not in front_cols]
    src_for_merge = src_for_merge.select(*(front_cols + other_cols))

    upsert(silver_tbl, src_for_merge)

    print("✅ Upserted & Registered in Hive Metastore.")
    spark.stop()


if __name__ == "__main__":
    main()
