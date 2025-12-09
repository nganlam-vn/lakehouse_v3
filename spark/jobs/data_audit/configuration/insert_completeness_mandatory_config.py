from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, FloatType, TimestampType, IntegerType
from datetime import datetime

BUCKET = "warehouse"
DATAAUDIT_PREFIX = "dataaudit/mandatory_column_configuration"
TABLE_PATH = f"s3a://{BUCKET}/{DATAAUDIT_PREFIX}"


def overwrite_table(spark: SparkSession, df_new):
    """
    Always overwrite the table with new data (including schema)
    """
    # Write with overwrite mode + overwrite schema
    df_new.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(TABLE_PATH)
    
    print(f"✅ Overwritten successfully! Total records = {df_new.count()}")


def main():
    spark = SparkSession.builder.appName("InsertMandatoryColumnConfiguration").getOrCreate()

    sample_data = [
        # CoinMarketCap
        (
            1,
            "delta",
            "bronze2",
            "coinmarketcap",
            "max_supply",
            "cd_bronze_id,name,symbol",
            None,
            "dt_utc_record_to_bronze",
            7.0,
            "Check if max_supply is NULL for coinmarketcap data",
            True,
            datetime.now()
        ),
        # CoinMarketCap
        (
            2,
            "delta",
            "bronze2",
            "coinmarketcap",
            "quote.usd.market_cap_dominance,quote.usd.percent_change_1h,quote.usd.percent_change_24,quote.usd.percent_change_30d,quote.usd.percent_change_60,quote.usd.percent_change_90d,quote.usd.price",
            "cd_bronze_id,name,symbol",
            None,
            "dt_utc_record_to_bronze",
            7.0,
            "Check mandatory price fields for coinmarketcap data",
            True,
            datetime.now()
        ),
        # CoinMarketCap
        (
            3,
            "delta",
            "bronze2",
            "coinmarketcap",
            "self_reported_circulating_supply,self_reported_market_cap",
            "cd_bronze_id,name,symbol",
            None,
            "dt_utc_record_to_bronze",
            7.0,
            "Check mandatory self-reported fields for coinmarketcap data",
            True,
            datetime.now()
        ),
        (
            4,
            "delta",
            "silver",
            "uber_bookings",
            "payment_method",
            "cd_silver_id,booking_id",
            None,
            "time",
            7.0,
            "Check if payment_method is NULL for uber_bookings data",
            True,
            datetime.now()
        ),
    ]
    
    schema = StructType([
        StructField("cd_id_configuration", IntegerType(), True),
        StructField("ds_catalog_name", StringType(), True),
        StructField("ds_schema_name", StringType(), True),
        StructField("ds_table_name", StringType(), True),
        StructField("ds_mandatory_column_array", StringType(), True),
        StructField("ds_PK_column_array", StringType(), True),
        StructField("ds_additional_filter_condition", StringType(), True),
        StructField("ds_utc_timestamp_column", StringType(), True),
        StructField("nr_timezone", FloatType(), True),
        StructField("ds_rule_description", StringType(), True),
        StructField("fl_is_active", BooleanType(), True),
        StructField("created_at", TimestampType(), True)
    ])

    # Create DataFrame
    df_new = spark.createDataFrame(sample_data, schema=schema)

    print("\n📊 Data to be inserted (will overwrite existing):")
    df_new.show(truncate=False)

    print("\n💾 Overwriting table (including schema)...")
    overwrite_table(spark, df_new)

    print("\n✅ Verifying...")
    df_result = spark.read.format("delta").load(TABLE_PATH)
    df_result.show(truncate=False)
    print(f"📈 Total records = {df_result.count()}")

    spark.stop()


if __name__ == "__main__":
    main()