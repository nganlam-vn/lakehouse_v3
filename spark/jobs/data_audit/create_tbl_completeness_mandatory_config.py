from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, FloatType, TimestampType
from datetime import datetime

BUCKET = "warehouse"
DATAAUDIT_PREFIX = "dataaudit/mandatory_column_configuration"

def create_mandatory_column_configuration_table(spark: SparkSession):

    spark.sql("CREATE DATABASE IF NOT EXISTS dataaudit")
    try:
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS dataaudit.mandatory_column_configuration(
            ds_catalog_name STRING,
            ds_schema_name STRING,
            ds_table_name STRING,
            ds_mandatory_column_array STRING,
            ds_PK_column_array STRING,
            ds_additional_filter_condition STRING,
            ds_utc_timestamp_column STRING,
            nr_timezone FLOAT,
            ds_rule_description STRING,
            fl_is_active BOOLEAN,
            created_at TIMESTAMP
        )
        USING DELTA
        LOCATION 's3a://{BUCKET}/{DATAAUDIT_PREFIX}'
        """)
    except Exception as e:
        print(f"Error creating table: {e}")


def main():
    spark = SparkSession.builder.appName("CreateMandatoryColumnConfigurationTable").getOrCreate()
    create_mandatory_column_configuration_table(spark)
    spark.stop()

if __name__ == "__main__":
    main()