from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from tasks.uber.ingestion import run_generation_for_date as ingest_uber_data
# Timezone (Việt Nam)
TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 10,                          
    "retry_delay": timedelta(minutes=1),        
}

with DAG(
    dag_id="UBER",
    description="Bronze1 CSV -> Bronze2 Delta -> Silver -> Gold",
    start_date=pendulum.datetime(2024, 1, 1, tz=TZ),
    #schedule="*/5 * * * *",                     # chạy mỗi 5 phút
    catchup=False,
    max_active_runs=1,                          # không cho chạy chồng
    default_args=DEFAULT_ARGS,
    tags=["uber"],
) as dag:
    
    ingest_task = PythonOperator(
        task_id="ingest_uber_data",
        python_callable=ingest_uber_data,
        op_kwargs={"execution_date_str": "{{ macros.ds_add(ds, -1) }}"},
        #op_kwargs={"execution_date_str": "{{ ds }}"},
    )

    transform_to_delta = BashOperator(
        task_id="csv_to_delta",
        bash_command=(
            'docker exec delta-spark bash -lc '
            '"spark-submit --master local[*] '
            '--packages io.delta:delta-spark_2.13:4.0.0,org.apache.hadoop:hadoop-aws:3.4.1 '
            '--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 '
            '--conf spark.hadoop.fs.s3a.access.key=admin '
            '--conf spark.hadoop.fs.s3a.secret.key=password '
            '--conf spark.hadoop.fs.s3a.path.style.access=true '
            '--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem '
            '--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false '
            '--conf spark.sql.warehouse.dir=s3a://warehouse/ '
            '--conf spark.sql.catalogImplementation=hive '
            '--conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 '
            '--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension '
            '--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog '
            '/opt/jobs/uber/csv_to_delta.py"'
        ),
    )

    transform_to_silver = BashOperator(
        task_id="bronze2_to_silver",
        bash_command=(
            'docker exec delta-spark bash -lc '
            '"spark-submit --master local[*] '
            '--packages io.delta:delta-spark_2.13:4.0.0,org.apache.hadoop:hadoop-aws:3.4.1 '
            '--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 '
            '--conf spark.hadoop.fs.s3a.access.key=admin '
            '--conf spark.hadoop.fs.s3a.secret.key=password '
            '--conf spark.hadoop.fs.s3a.path.style.access=true '
            '--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem '
            '--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false '
            '--conf spark.sql.warehouse.dir=s3a://warehouse/ '
            '--conf spark.sql.catalogImplementation=hive '
            '--conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 '
            '--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension '
            '--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog '
            '/opt/jobs/uber/bronze2_to_silver.py"'
        ),
    )

    transform_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            'docker exec delta-spark bash -lc '
            '"spark-submit --master local[*] '
            '--packages io.delta:delta-spark_2.13:4.0.0,org.apache.hadoop:hadoop-aws:3.4.1 '
            '--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 '
            '--conf spark.hadoop.fs.s3a.access.key=admin '
            '--conf spark.hadoop.fs.s3a.secret.key=password '
            '--conf spark.hadoop.fs.s3a.path.style.access=true '
            '--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem '
            '--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false '
            '--conf spark.sql.warehouse.dir=s3a://warehouse/ '
            '--conf spark.sql.catalogImplementation=hive '
            '--conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 '
            '--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension '
            '--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog '
            '/opt/jobs/uber/silver_to_gold.py"'
        ),
    )

    ingest_task >> transform_to_delta >> transform_to_silver >> transform_to_gold