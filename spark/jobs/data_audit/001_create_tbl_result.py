from pyspark.sql import SparkSession

BUCKET = "warehouse"
DATAAUDIT_PREFIX = "dataaudit/bronze_dataaudit_result"

def create_dataaudit_result_table(spark: SparkSession):

    spark.sql("CREATE DATABASE IF NOT EXISTS dataaudit")
    try:
        # ✅ FIXED: Removed DEFAULT - not supported in Delta Lake
        # Let Delta infer schema from data instead
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS dataaudit.bronze_dataaudit_result
        USING DELTA
        LOCATION 's3a://{BUCKET}/{DATAAUDIT_PREFIX}'
        """)
        print("✅ Table created successfully!")
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        raise


def main():
    spark = SparkSession.builder.appName("CreateDataauditResultTable").getOrCreate() 
    
    create_dataaudit_result_table(spark)

    print("\n=== Verify table registered in Hive Metastore ===")
    spark.sql("SHOW TABLES IN dataaudit").show()

    print("\n=== Table Schema ===")
    spark.sql("DESCRIBE dataaudit.bronze_dataaudit_result").show(truncate=False)

    # print("\n=== Read table content ===")
    # spark.sql("SELECT * FROM dataaudit.bronze_dataaudit_result").show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()