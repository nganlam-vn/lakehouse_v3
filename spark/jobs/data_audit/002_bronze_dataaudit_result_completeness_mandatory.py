from pyspark.sql import SparkSession
from datetime import datetime, timedelta, timezone
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import uuid
from pyspark.sql.functions import expr, col
import pyspark.sql.functions as F
import json
from functools import reduce

BUCKET = "warehouse"
DATAAUDIT_PREFIX = "dataaudit/mandatory_column_configuration"
RESULTS_PATH = f"s3a://{BUCKET}/dataaudit/bronze_dataaudit_result" 
TABLE_PATH = f"s3a://{BUCKET}/{DATAAUDIT_PREFIX}"
now = datetime.now(timezone.utc)
past_1_hour = now - timedelta(hours=2000)
start_timestamp = past_1_hour.replace( minute=0, second=0, microsecond=0)
end_timestamp = start_timestamp + timedelta(hours=2000)
print(start_timestamp)
print(end_timestamp)

result_records = []
LIMIT_VAL = 999

def save_audit_results(spark: SparkSession, result_records):
    """
    Save audit results with auto-generated UUID and timestamp
    """
    if not result_records:
        print("⚠️ No results to save")
        return
    
    # Schema (without UUID and timestamp - will add them)
    schema = StructType([
        StructField("ds_configuration", StringType(), True),
        StructField("ds_checked_value", StringType(), True),
        StructField("nr_status", IntegerType(), True)
    ])
    
    # Create DataFrame from records
    df = spark.createDataFrame(result_records, schema=schema)
    
    # Add UUID (unique for each row)
    df = df.withColumn("cd_dataaudit_result", expr("uuid()"))

    df = df.withColumn("dt_checked_at", expr("current_timestamp()"))
   
    
    # Reorder columns
    df_final = df.select(
        "cd_dataaudit_result",
        "ds_configuration",
        "ds_checked_value",
        "nr_status",
        "dt_checked_at"
    )
    
    print("\n" + "="*80)
    print("AUDIT RESULTS (with auto-generated UUID and timestamp):")
    print("="*80)
    df_final.show(truncate=False)
    
    # Save to Delta Lake
    df_final.write.format("delta").mode("append").option("mergeSchema", "true").save(RESULTS_PATH)

    print(f"✅ Results saved to: {RESULTS_PATH}")


def completeness_mandatory_column_audit(spark: SparkSession):
    df_config = spark.read.format("delta").load(TABLE_PATH)
    for row in df_config.collect():
        if row["fl_is_active"] == True:
            status = -1
            id_configuration = row["cd_id_configuration"]
            schema_name = row["ds_schema_name"]
            table_name = row["ds_table_name"]
            mandatory_column_array = row["ds_mandatory_column_array"] or ""
            mandatory_columns = [
                c.strip()
                for c in mandatory_column_array.split(",")
                if c and c.strip()
            ]
            additional_filter_condition = row["ds_additional_filter_condition"] or ""
            nr_timezone = row["nr_timezone"]
            utc_timestamp_column = row["ds_utc_timestamp_column"] 
            pk_column_array = row["ds_PK_column_array"] or ""
            pk_columns = [
                c.strip()
                for c in pk_column_array.split(",")
                if c and c.strip()
            ]
            rule_description = row["ds_rule_description"]
            # id_configuration = row["id_configuration"]
            try:

                def json_result(compact_pk, total_pk, status, note):
                    ds_configuration = json.dumps({
                        "dimension": "completeness_mandatory_column",
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "id_configuration": id_configuration,
                        "timestamp_utc_column": utc_timestamp_column ,
                        "mandatory_column_array": mandatory_columns,
                        "dbx_pk": pk_columns ,
                        "additional_filter_condition": additional_filter_condition ,
                        "rule_description": rule_description 
                })
                                        
                    ds_checked_value = json.dumps({
                        "number_of_violated_rows": total_pk,
                        "dbx_pk_result":compact_pk,
                        "note": note
                    })
                                        
                    result_records.append({
                        "ds_configuration": ds_configuration,
                        "ds_checked_value": ds_checked_value,
                        "nr_status": status
                    })  
            
                #declare query
                if additional_filter_condition:
                    select_query =f""" SELECT {pk_column_array}
                    FROM {schema_name}.{table_name}
                    WHERE {additional_filter_condition}
                    AND {utc_timestamp_column} >= '{start_timestamp}' 
                    AND {utc_timestamp_column} < '{end_timestamp}'
                """
                else:
                    select_query =f""" SELECT {pk_column_array}
                    FROM {schema_name}.{table_name}
                    WHERE {utc_timestamp_column} >= '{start_timestamp}' 
                    AND {utc_timestamp_column} < '{end_timestamp}'"""

                final_query = f"""{select_query}"""

                df_result = spark.sql(final_query)

                if mandatory_columns:
                    condition_violation = reduce(
                        lambda a, b: a | b,
                         [(F.col(c).isNull() | (F.trim(F.col(c)) == "")) for c in mandatory_columns]
                    )
                else:
                    json_result(compact_pk = [], total_pk = 0, status = -1, note = "ERROR! No mandatory columns defined")
                    print(f"No mandatory columns defined for table {table_name}. Skipping audit.")
                    continue

                #apply filter and get PK
                agg_pk = df_result.filter(condition_violation).agg(
                    F.collect_list(F.struct(*pk_columns)).alias("compact_pk"),
                    F.count("*").alias("count_pk")
                ).first()

                #count and set nr_status
                total_null = int(agg_pk["count_pk"])
                status = 0 if total_null > 0 else 1

                #LIMIT
                if total_null <= LIMIT_VAL:
                    # total_null = 999
                    compact_pk= agg_pk["compact_pk"] or []
                else:
                    compact_pk = agg_pk["compact_pk"][:LIMIT_VAL]

                json_result(compact_pk = compact_pk, total_pk = total_null, status = status, note = None)

            except Exception as e:
                print(f"Error processing id_configuration {id_configuration}: {e}")

    if result_records:
        save_audit_results(spark, result_records)
    else:
        print("No tables to process.")


def main():
    spark = SparkSession.builder.appName("InsertMandatoryColumnConfiguration").getOrCreate()
    completeness_mandatory_column_audit(spark)
    spark.stop()


if __name__ == "__main__":
    main()
