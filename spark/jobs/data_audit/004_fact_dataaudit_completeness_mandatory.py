from pyspark.sql import SparkSession

BUCKET = "warehouse"
FACT_PREFIX = "dataaudit/fact_dataaudit_completeness_mandatory_column"


def create_fact_table(spark: SparkSession):
    """
    Create fact table using SQL (without unsupported features)
    """
    spark.sql("CREATE DATABASE IF NOT EXISTS dataaudit")
    
    try:
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS dataaudit.fact_dataaudit_completeness_mandatory_column (
            cd_fact_dataaudit_completeness_mandatory_column BIGINT,
            dt_record_to_fact TIMESTAMP,
            ds_bronze_cd_dataaudit_result STRING,
            ds_dimension STRING,
            nr_id_configuration INT,
            ds_schema STRING,
            ds_table STRING,
            ds_timestamp_utc_column STRING,
            ds_mandatory_column_array STRING,
            ds_additional_filter_condition STRING,
            ds_rule_description STRING,
            ds_pk STRING,
            ds_violated_records STRING,
            nr_total_violated_records INT,
            ds_note STRING,
            ds_audit_result STRING,
            dt_checked_at TIMESTAMP
        )
        USING DELTA
        LOCATION 's3a://{BUCKET}/{FACT_PREFIX}'
        """)
        print("Table created successfully!")
    except Exception as e:
        print(f"Error creating table: {e}")
        raise


def transform_to_fact_table(spark: SparkSession):
    """
    Transform bronze to fact using SQL
    """
    print("\n=== Starting transformation ===")
    
    # Get max ID for auto-increment
    max_id = spark.sql("""
        SELECT COALESCE(MAX(cd_fact_dataaudit_completeness_mandatory_column), 0) AS max_id
        FROM dataaudit.fact_dataaudit_completeness_mandatory_column
    """).first()["max_id"]
    
    print(f"📊 Current max ID: {max_id}")
  
    spark.sql(f"""
        WITH latest_fact AS (
            SELECT COALESCE(MAX(dt_checked_at), TIMESTAMP('2025-11-03 03:26:00')) AS max_checked_at
            FROM dataaudit.fact_dataaudit_completeness_mandatory_column
        ),
        
        src_parsed AS (
            SELECT
                *,
                from_json(ds_configuration,
                    'STRUCT<
                        dimension: STRING,
                        catalog_name: STRING,
                        schema_name: STRING,
                        table_name: STRING,
                        id_configuration: INT,
                        timestamp_utc_column: STRING,
                        dbx_pk: STRING,
                        additional_filter_condition: STRING,
                        mandatory_column_array: ARRAY<STRING>,
                        rule_description: STRING
                    >'
                ) AS configuration,
                
                from_json(ds_checked_value,
                    'STRUCT<
                        number_of_violated_rows: INT,
                        dbx_pk_result: ARRAY<STRING>,
                        note: STRING
                    >'
                ) AS checked_value,
                
                CASE 
                    WHEN nr_status = 1 THEN 'PASS'
                    WHEN nr_status = 0 THEN 'FAIL'
                    WHEN nr_status = -1 THEN 'ERROR'
                    ELSE CAST(nr_status AS STRING)
                END AS ds_audit_result
                
            FROM dataaudit.bronze_dataaudit_result
            WHERE ds_configuration LIKE '%completeness_mandatory_column%'
              AND dt_checked_at > (SELECT max_checked_at FROM latest_fact)
        ),
        
        exploded_records AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY dt_checked_at) + {max_id} AS cd_fact_dataaudit_completeness_mandatory_column,
                CURRENT_TIMESTAMP() AS dt_record_to_fact,
                cd_dataaudit_result AS ds_bronze_cd_dataaudit_result,
                configuration.dimension AS ds_dimension,
                configuration.id_configuration AS nr_id_configuration,
                configuration.schema_name AS ds_schema,
                configuration.table_name AS ds_table,
                configuration.timestamp_utc_column AS ds_timestamp_utc_column,
                configuration.mandatory_column_array AS ds_mandatory_column_array,
                configuration.additional_filter_condition AS ds_additional_filter_condition,
                configuration.rule_description AS ds_rule_description,
                configuration.dbx_pk AS ds_pk,
                checked_value.number_of_violated_rows AS nr_total_violated_records,
                explode_outer(checked_value.dbx_pk_result) AS ds_violated_records,
                checked_value.note AS ds_note,
                ds_audit_result,
                dt_checked_at
            FROM src_parsed
        )
        INSERT INTO dataaudit.fact_dataaudit_completeness_mandatory_column (
            cd_fact_dataaudit_completeness_mandatory_column,
            dt_record_to_fact,
            ds_bronze_cd_dataaudit_result,
            ds_dimension,
            nr_id_configuration,
            ds_schema,
            ds_table,
            ds_timestamp_utc_column,
            ds_mandatory_column_array,
            ds_additional_filter_condition,
            ds_rule_description,
            ds_pk,
            ds_violated_records,
            nr_total_violated_records,
            ds_note,
            ds_audit_result,
            dt_checked_at
        )
        SELECT
            cd_fact_dataaudit_completeness_mandatory_column,
            dt_record_to_fact,
            ds_bronze_cd_dataaudit_result,
            ds_dimension,
            nr_id_configuration,
            ds_schema,
            ds_table,
            ds_timestamp_utc_column,
            ds_mandatory_column_array,
            ds_additional_filter_condition,
            ds_rule_description,
            ds_pk,
            ds_violated_records,
            nr_total_violated_records,
            ds_note,
            ds_audit_result,
            dt_checked_at
        FROM exploded_records
    """)
    
    print("Inserted to dataaudit.fact_dataaudit_completeness_mandatory_column!")


def main():
    spark = SparkSession.builder.appName("FactDataauditCompletenessMandatory").getOrCreate()
    
    # Create table
    create_fact_table(spark)
    
    print("\n=== Verify table registered in Hive Metastore ===")
    spark.sql("SHOW TABLES IN dataaudit").show()
    
    print("\n=== Table Schema ===")
    spark.sql("DESCRIBE dataaudit.fact_dataaudit_completeness_mandatory_column").show(truncate=False)
    
    # Transform data
    transform_to_fact_table(spark)
    
    # Verify results
    print("\n=== Fact Table Summary ===")
    spark.sql("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT ds_bronze_cd_dataaudit_result) as unique_audits,
            SUM(nr_total_violated_records) as total_violations,
            MAX(dt_checked_at) as latest_audit
        FROM dataaudit.fact_dataaudit_completeness_mandatory_column
    """).show(truncate=False)
    
    print("\n=== Sample Records ===")
    spark.sql("""
        SELECT * 
        FROM dataaudit.fact_dataaudit_completeness_mandatory_column 
        ORDER BY cd_fact_dataaudit_completeness_mandatory_column DESC 
        LIMIT 5
    """).show(truncate=False, vertical=True)
    
    spark.stop()


if __name__ == "__main__":
    main()