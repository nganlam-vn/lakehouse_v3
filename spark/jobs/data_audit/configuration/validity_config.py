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
            "cd_bronze_id,name,symbol",
            "dt_record_to_bronze",
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
            "cd_bronze_id,name,symbol",
            "dt_record_to_bronze",
            7.0,
            "Check if USD price is negative for coinmarketcap data. That's invalid.",
            True,
            datetime.now()
        ),
             
        (
            3,
            "delta",
            "bronze2",
            "coinmarketcap",
            "quote.usd.percent_change_1h < -10 OR quote.usd.percent_change_24h < -10 OR quote.usd.percent_change_7d < -10 OR quote.usd.percent_change_30d < -10 OR quote.usd.percent_change_60d < -10 OR quote.usd.percent_change_90d < -10",
            "cd_bronze_id,name,symbol",
            "dt_record_to_bronze",
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
            "dt_record_to_bronze",
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
            "high < low",
            "cd_bronze_id,symbol",
            "dt_record_to_bronze",
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
            "dt_record_to_bronze",
            0.0,
            "Check if volume is negative for stock data. It should be non-negative.",
            True,
            datetime.now()
        ),
        # Silver Stocks - Avg price must be between low and high
        (
            7,
            "delta",
            "bronze2",
            "stocks_intraday",
            "avg_price < low OR avg_price > high",
            "cd_bronze_id,symbol",
            "dt_record_to_bronze",
            0.0,
            "Check if avg_price is not between low and high for silver stocks data.",
            True,
            datetime.now()
        ),
        (
            8, 
            "delta", 
            "bronze2", 
            "coinmarketcap", 
            "quote.usd.volume_24 <= 1", 
            "cd_bronze_id,name,symbol", 
            "dt_record_to_bronze", 
            7.0, 
            "Check if 24h Volume is negative (Invalid)", 
            True, 
            datetime.now()
        ),
        (
            9, 
            "delta", 
            "bronze2", 
            "coinmarketcap", 
            "circulating_supply > max_supply", 
            "cd_bronze_id,name,symbol", 
            "dt_record_to_bronze", 
            7.0, 
            "Check if Circulating Supply > Max Supply (Invalid)", 
            True, 
            datetime.now()
        ),
        (
            10, 
            "delta", 
            "bronze2", 
            "uber_bookings", 
            "ride_distance < 0", 
            "cd_bronze_id,booking_id", 
            "dt_record_to_bronze", 
            7.0, 
            "Check if ride distance is negative (Invalid)", 
            True, 
            datetime.now()
        ),
        (
            11, 
            "delta", 
            "bronze2", 
            "uber_bookings", 
            "booking_value < 0", 
            "cd_bronze_id,booking_id", 
            "dt_record_to_bronze", 
            7.0, 
            "Check if booking value/fare is negative (Invalid)", 
            True, 
            datetime.now()
        ),
        (
            12, 
            "delta", 
            "bronze2", 
            "uber_bookings", 
            "lower(booking_status) = 'completed' AND (booking_value IS NULL OR booking_value = 0)", 
            "cd_bronze_id,booking_id", 
            "dt_record_to_bronze", 
            7.0, 
            "Check completed rides with 0 or null value (Invalid)", 
            True, 
            datetime.now()
        ),
        (
            13, 
            "delta", 
            "bronze2", 
            "finance_news", 
            "length(title) <= 100", 
            "cd_bronze_id,url", 
            "dt_record_to_bronze", 
            7.0, 
            "Check if title is suspicious/too short (Invalid)", 
            True, 
            datetime.now()
        ),
        (
            14, 
            "delta", 
            "bronze2", 
            "uber_bookings", 
            "length(customer_id) <> 16 OR length(booking_id) <> 16", 
            "cd_bronze_id,booking_id", 
            "dt_record_to_bronze", 
            7.0, 
            "Check if id fields is suspicious/too short (Invalid)", 
            True, 
            datetime.now()
        ),
        # Uber Silver: Kiểm tra cột 'hour' (Derived Column)
        (
            15, 
            "delta", 
            "silver", 
            "uber_bookings", 
            "hour < 0 OR hour > 23", 
            "cd_silver_id,booking_id", 
            "dt_record_to_silver", 
            7.0, 
            "Check if extracted Hour is invalid (Must be 0-23)", 
            True, 
            datetime.now()
        ),
        
        # Uber Silver: Kiểm tra cột 'rushhour' (Flag Validity)
        (
            16, 
            "delta", 
            "silver", 
            "uber_bookings", 
            "rushhour NOT IN (0, 1)", 
            "cd_silver_id,booking_id", 
            "dt_record_to_silver", 
            7.0, 
            "Check if Rushhour flag is invalid (Must be 0 or 1)", 
            True, 
            datetime.now()
        ),

        # CMC Bronze2: Kiểm tra Dominance (Logic Percent)
        (
            17, 
            "delta", 
            "bronze2", 
            "coinmarketcap", 
            "quote.usd.market_cap_dominance > 100", 
            "cd_bronze_id,symbol", 
            "dt_record_to_bronze", 
            7.0, 
            "Check if Market Dominance > 100% (Impossible)", 
            True, 
            datetime.now()
        ),

        # CMC Bronze2: Kiểm tra Rank (Logic Integer)
        (
            18, 
            "delta", 
            "bronze2", 
            "coinmarketcap", 
            "cmc_rank <= 0", 
            "cd_bronze_id,symbol", 
            "dt_record_to_bronze", 
            7.0, 
            "Check if CMC Rank is non-positive (Invalid)", 
            True, 
            datetime.now()
        ),
        (
            19, 
            "delta", 
            "bronze2", 
            "uber_bookings", 
            "driver_ratings < 1.0 OR driver_ratings > 5.0", 
            "cd_bronze_id,booking_id", 
            "dt_record_to_bronze", 
            7.0, 
            "Check Driver Rating Validity (Must be 1.0 - 5.0)", 
            True, 
            datetime.now()
        ),
        (
            20, 
            "delta", 
            "bronze2", 
            "uber_bookings", 
            "customer_rating < 1.0 OR customer_rating > 5.0", 
            "cd_bronze_id,booking_id", 
            "dt_record_to_bronze", 
            7.0, 
            "Check Customer Rating Validity (Must be 1.0 - 5.0)", 
            True, 
            datetime.now()
        ),
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