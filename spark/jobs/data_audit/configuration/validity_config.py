from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, FloatType, TimestampType, IntegerType
from datetime import datetime

BUCKET = "warehouse"
DATAAUDIT_PREFIX = "dataaudit/validity_configuration"
TABLE_PATH = f"s3a://{BUCKET}/{DATAAUDIT_PREFIX}"


def create_validity_configuration_table(spark: SparkSession):
    """
    Create validity_configuration table if not exists
    """
    spark.sql("CREATE DATABASE IF NOT EXISTS dataaudit")
    
    try:
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS dataaudit.validity_configuration(
            cd_id_configuration INT,
            ds_catalog_name STRING,
            ds_schema_name STRING,
            ds_table_name STRING,
            ds_validation_rule STRING,
            ds_PK_column_array STRING,
            ds_utc_timestamp_column STRING,
            nr_timezone FLOAT,
            ds_rule_description STRING,
            fl_is_active BOOLEAN,
            created_at TIMESTAMP
        )
        USING DELTA
        LOCATION '{TABLE_PATH}'
        """)
        print("✅ Table dataaudit.validity_configuration created successfully.")
    except Exception as e:
        print(f"❌ Error creating table: {e}")


def overwrite_table(spark: SparkSession, df_new):
    """
    Always overwrite the table with new data (including schema)
    """
    df_new.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(TABLE_PATH)
    
    print(f"✅ Overwritten successfully! Total records = {df_new.count()}")


def insert_sample_data(spark: SparkSession):
    """
    Insert sample validity configuration data
    """
    sample_data = [
        # CoinMarketCap - Price must be positive
        (
            1,
            "delta",
            "bronze2",
            "coinmarketcap",
            "quote.usd.price < 0",
            "cd_bronze2_id,name,symbol",
            "dt_utc_record_to_bronze2",
            7.0,
            "Check if USD price is negative for coinmarketcap data. That's invalid.",
            True,
            datetime.now()
        ),
        # CoinMarketCap - Market cap must be positive
        (
            2,
            "delta",
            "bronze2",
            "coinmarketcap",
            "quote.usd.market_cap < 0",
            "cd_bronze2_id,name,symbol",
            "dt_utc_record_to_bronze2",
            7.0,
            "Check if USD price is negative for coinmarketcap data. That's invalid.",
            True,
            datetime.now()
        ),
                # CoinMarketCap - Market cap must be positive
        (
            3,
            "delta",
            "bronze2",
            "coinmarketcap",
            "quote.usd.percent_change_1h < 0 OR quote.usd.percent_change_24h < 0 OR quote.usd.percent_change_30d < 0 OR quote.usd.percent_change_60d < 0 OR quote.usd.percent_change_90d < 0",
            "cd_bronze2_id,name,symbol",
            "dt_utc_record_to_bronze2",
            7.0,
            "Check if USD percent_change in hours is negative for coinmarketcap data. That's invalid.",
            True,
            datetime.now()
        ),

        # Stocks - Open price must be positive
        (
            4,
            "delta",
            "bronze2",
            "stocks_intraday",
            "open < 0",
            "cd_bronze_id,symbol",
            "timestamp",
            0.0,
            "Check if open price is negative for stock data. That's invalid.",
            True,
            datetime.now()
        ),
        # Stocks - High >= Low
        (
            5,
            "delta",
            "bronze2",
            "stocks_intraday",
            "high >= low",
            "cd_bronze_id,symbol",
            "timestamp",
            0.0,
            "Check if high price is greater than or equal to low price. That's invalid.",
            True,
            datetime.now()
        ),
        # Stocks - Volume must be non-negative
        (
            6,
            "delta",
            "bronze2",
            "stocks_intraday",
            "volume < 0",
            "cd_bronze_id,symbol",
            "timestamp",
            0.0,
            "Check if volume is negative for stock data. It should be non-negative.",
            True,
            datetime.now()
        ),
        # Silver Stocks - Avg price must be between low and high
        (
            7,
            "delta",
            "silver",
            "stocks_intraday",
            "avg_price < low AND avg_price > high",
            "cd_silver_id,symbol",
            "timestamp",
            0.0,
            "Check if avg_price is not between low and high for silver stocks data.",
            True,
            datetime.now()
        )
    ]
    
    schema = StructType([
        StructField("cd_id_configuration", IntegerType(), True),
        StructField("ds_catalog_name", StringType(), True),
        StructField("ds_schema_name", StringType(), True),
        StructField("ds_table_name", StringType(), True),
        StructField("ds_validation_rule", StringType(), True),
        StructField("ds_PK_column_array", StringType(), True),
        StructField("ds_utc_timestamp_column", StringType(), True),
        StructField("nr_timezone", FloatType(), True),
        StructField("ds_rule_description", StringType(), True),
        StructField("fl_is_active", BooleanType(), True),
        StructField("created_at", TimestampType(), True)
    ])

    df_new = spark.createDataFrame(sample_data, schema=schema)

    print("\n📊 Data to be inserted (will overwrite existing):")
    df_new.show(truncate=False)

    print("\n💾 Overwriting table (including schema)...")
    overwrite_table(spark, df_new)


def main():
    spark = SparkSession.builder.appName("CreateValidityConfigurationTable").getOrCreate() 
    
    # Step 1: Create table
    create_validity_configuration_table(spark)
    
    # Step 2: Insert sample data
    insert_sample_data(spark)

    # Step 3: Verify
    print("\n=== Verify table registered in Hive Metastore ===")
    spark.sql("SHOW TABLES IN dataaudit").show()

    print("\n=== Read table content ===")
    df_result = spark.sql("SELECT * FROM dataaudit.validity_configuration")
    df_result.show(truncate=False)
    print(f"📈 Total records = {df_result.count()}")

    spark.stop()


if __name__ == "__main__":
    main()