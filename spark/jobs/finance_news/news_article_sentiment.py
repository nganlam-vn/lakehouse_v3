# /opt/jobs/finance_news/news_article_sentiment_gold.py
import os
import re
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable

BUCKET = os.getenv("BUCKET", "warehouse")

SRC_DB = os.getenv("SRC_DB", "silver")
SRC_TBL = os.getenv("SRC_TBL", "finance_news")

GOLD_DB = os.getenv("GOLD_DB", "gold")
GOLD_TBL = os.getenv("GOLD_TBL", "news_article_sentiment")
GOLD_PATH = os.getenv("GOLD_PATH", f"s3a://{BUCKET}/{GOLD_DB}/{GOLD_TBL}")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() in ("1", "true", "yes", "y")

DAYS_BACK = int(os.getenv("DAYS_BACK", "7"))


POSITIVE_WORDS = {
    "gain", "gains", "rise", "rises", "surge", "surges", "up", "higher",
    "beat", "beats", "profit", "profits", "growth", "strong", "bullish",
    "upgrade", "upgraded", "record", "rally", "optimistic"
}

NEGATIVE_WORDS = {
    "loss", "losses", "fall", "falls", "plunge", "plunges", "down", "lower",
    "miss", "misses", "weak", "weakness", "bearish", "downgrade", "downgraded",
    "warning", "lawsuit", "fraud", "default", "bankrupt", "bankruptcy"
}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("news_article_sentiment_gold")
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
        .getOrCreate()
    )


def ensure_meta(spark: SparkSession):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")
    if DeltaTable.isDeltaTable(spark, GOLD_PATH):
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {GOLD_DB}.{GOLD_TBL} "
            f"USING DELTA LOCATION '{GOLD_PATH}'"
        )


def get_max_gold_id(spark: SparkSession) -> int:
    if not DeltaTable.isDeltaTable(spark, GOLD_PATH):
        return 0
    from pyspark.sql.functions import max as fmax
    m = (
        spark.read.format("delta").load(GOLD_PATH)
        .agg(fmax("cd_gold_id"))
        .collect()[0][0]
    )
    return int(m) if m is not None else 0


def read_silver_incremental(spark: SparkSession):
    df = spark.table(f"{SRC_DB}.{SRC_TBL}")
    df = df.filter(
        F.col("article_id").isNotNull()
        & F.col("event_date").isNotNull()
    )

    if DAYS_BACK > 0:
        lower = F.date_sub(F.current_date(), DAYS_BACK)
        upper = F.current_date()
        df = df.filter(
            (F.col("event_date") >= lower) & (F.col("event_date") < upper)
        )

    return df


def register_udf(spark: SparkSession):
    pos = POSITIVE_WORDS
    neg = NEGATIVE_WORDS

    def simple_score(text: str) -> float:
        if text is None:
            return 0.0
        txt = re.sub(r"[^a-zA-Z]+", " ", text).lower()
        tokens = txt.split()
        if not tokens:
            return 0.0
        pos_count = sum(1 for t in tokens if t in pos)
        neg_count = sum(1 for t in tokens if t in neg)
        if pos_count == 0 and neg_count == 0:
            return 0.0
        score = (pos_count - neg_count) / float(pos_count + neg_count)
        return float(score)

    spark.udf.register("simple_sentiment_score", simple_score)


def compute_sentiment(spark: SparkSession, df):
    register_udf(spark)

    text_col = F.coalesce(
        F.col("content").cast("string"),
        F.col("description").cast("string"),
        F.col("title").cast("string"),
    )

    df_scored = df.select(
        "article_id",
        "symbol",
        F.col("event_date").cast("date").alias("event_date"),
        text_col.alias("text"),
    ).withColumn(
        "sentiment_score",
        F.expr("simple_sentiment_score(text)")
    )

    df_scored = df_scored.withColumn(
        "sentiment_label",
        F.when(F.col("sentiment_score") > 0.1, F.lit("positive"))
         .when(F.col("sentiment_score") < -0.1, F.lit("negative"))
         .otherwise(F.lit("neutral"))
    )

    return df_scored.drop("text")


def attach_ids_and_times(spark: SparkSession, df_sent):
    max_id = get_max_gold_id(spark)
    w = Window.orderBy("symbol", "event_date", "article_id")
    return (
        df_sent
        .withColumn(
            "dt_record_to_gold",
            F.date_format(
                F.to_utc_timestamp(F.current_timestamp(), "UTC"),
                "yyyy-MM-dd'T'HH:mm:ss'Z'"
            )
        )
        .withColumn("cd_gold_id", (F.row_number().over(w) + F.lit(max_id)).cast("long"))
        .withColumn("dt_partition", F.col("event_date"))
    )


def upsert_to_gold(spark: SparkSession, df_gold):
    if df_gold.rdd.isEmpty():
        print("No sentiment records to write.")
        return

    if not DeltaTable.isDeltaTable(spark, GOLD_PATH):
        (
            df_gold.write.format("delta")
            .partitionBy("dt_partition")
            .mode("overwrite")
            .save(GOLD_PATH)
        )
    else:
        delta_tbl = DeltaTable.forPath(spark, GOLD_PATH)
        (
            delta_tbl.alias("t")
            .merge(
                df_gold.alias("s"),
                "t.article_id = s.article_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {GOLD_DB}.{GOLD_TBL}
        USING DELTA
        LOCATION '{GOLD_PATH}'
        """
    )
    print(f"Upserted sentiment into {GOLD_DB}.{GOLD_TBL}")


def main():
    spark = build_spark()
    ensure_meta(spark)

    df_silver = read_silver_incremental(spark)
    if df_silver.rdd.isEmpty():
        print("No new finance_news rows.")
        spark.stop()
        return

    df_sent = compute_sentiment(spark, df_silver)
    if df_sent.rdd.isEmpty():
        print("No sentiment computed.")
        spark.stop()
        return

    df_gold = attach_ids_and_times(spark, df_sent)
    upsert_to_gold(spark, df_gold)
    spark.stop()


if __name__ == "__main__":
    main()
