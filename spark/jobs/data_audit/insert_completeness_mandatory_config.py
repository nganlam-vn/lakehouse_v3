from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, FloatType, TimestampType, IntegerType
from datetime import datetime

BUCKET = "warehouse"
DATAAUDIT_PREFIX = "dataaudit/mandatory_column_configuration"
TABLE_PATH = f"s3a://{BUCKET}/{DATAAUDIT_PREFIX}"


def insert_or_create(spark: SparkSession, df_new):
    try:
        # Try reading existing table
        df_existing = spark.read.format("delta").load(TABLE_PATH)

        # Union + drop duplicates
        df_merged = (
            df_existing.unionByName(df_new)
                       .dropDuplicates([
                           "ds_catalog_name",
                           "ds_schema_name",
                           "ds_table_name",
                           "ds_mandatory_column_array",
                           "ds_additional_filter_condition"
                       ])
        )

        # Write back (overwrite only the files, not schema)
        df_merged.write.format("delta").mode("overwrite").save(TABLE_PATH)

        print(f"[OK] Inserted (merged) successfully! Total records = {df_merged.count()}")

    except Exception:
        # Table does not exist, create new Delta table
        df_new.write.format("delta").mode("overwrite").save(TABLE_PATH)
        print("[OK] Table did not exist, created new table with initial data.")


def main():
    spark = SparkSession.builder.appName("InsertMandatoryColumnConfiguration").getOrCreate()

    sample_data_1 = [
        (
            1,
            "delta",                          
            "bronze2",                        
            "coinmarketcap",                  
            "slug,self_reported_circulating_supply,name",
            "cd_bronze2_id",                   
            None,
            "dt_utc_record_to_bronze2",
            7.0,
            "Check mandatory fields for coinmarketcap data",
            True,
            datetime.now()
        )
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
    df_new = spark.createDataFrame(sample_data_1, schema=schema)

    print("\nData to be inserted:")
    df_new.show(truncate=False)

    print("\nInserting data...")
    insert_or_create(spark, df_new)

    print("\nVerifying...")
    df_result = spark.read.format("delta").load(TABLE_PATH)
    df_result.show(truncate=False)
    print(f"Total records = {df_result.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
