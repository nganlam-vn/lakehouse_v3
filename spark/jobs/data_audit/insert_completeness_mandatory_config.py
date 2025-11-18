from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, FloatType, TimestampType
from datetime import datetime

BUCKET = "warehouse"
DATAAUDIT_PREFIX = "dataaudit/mandatory_column_configuration"


def merge_mandatory_column_configuration(spark: SparkSession, df_new):
    try:
        df_existing = spark.read.format("delta").load(f"s3a://{BUCKET}/{DATAAUDIT_PREFIX}")
        
        df_merged = df_existing.unionByName(df_new).dropDuplicates(
            subset=["ds_catalog_name", "ds_schema_name", "ds_table_name", "ds_mandatory_column_array", "ds_additional_filter_condition" ]
        )
        
        df_merged.write.format("delta").mode("overwrite").save(f"s3a://{BUCKET}/{DATAAUDIT_PREFIX}")
        print(f"Merged successfully! Total records: {df_merged.count()}")
    except Exception as e:
        # If table doesn't exist, create it with the new data
        print(f"Table doesn't exist yet. Creating new table. Error was: {e}")
        
        # Write the new data to create the Delta table
        df_new.write.format("delta").mode("overwrite").save(f"s3a://{BUCKET}/{DATAAUDIT_PREFIX}")
        print(f"Table created with {df_new.count()} records!")

def main():
    spark = SparkSession.builder.appName("InsertMandatoryColumnConfiguration").getOrCreate()

    sample_data_1 = [
        (
            "delta",                          # ds_catalog_name
            "bronze2",                        # ds_schema_name
            "coinmarketcap",                  # ds_table_name
            "[slug,self_reported_circulating_supply,name]",                 # ds_mandatory_column_array
            "cd_bronze2_id",                             # ds_PK_column_array
            None,        # ds_additional_filter_condition
            "dt_utc_record_to_bronze2",       # ds_utc_timestamp_column
            7.0,                            # nr_timezone
            "Check mandatory fields for coinmarketcap data",  # ds_rule_description
            True,                          # fl_is_active
            datetime.now()                 # created_at
        )
    ]
    
    schema = StructType([
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
        # Create DataFrame from sample data
    df_new = spark.createDataFrame(sample_data_1, schema=schema)
    print("\n Data to be inserted:")
    df_new.show(truncate=False)
    
    # Merge/insert the data
    print("\n Inserting data into Delta table...")
    merge_mandatory_column_configuration(spark, df_new)

    # Verify the data was inserted
    print("\nVerifying inserted data:")
    result_df = spark.read.format("delta").load(f"s3a://{BUCKET}/{DATAAUDIT_PREFIX}")
    result_df.show(truncate=False)
    
    print(f"\n Total configurations: {result_df.count()}")

    spark.stop()

if __name__ == "__main__":
    main()