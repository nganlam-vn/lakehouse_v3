from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

from tasks.ingestion.stock_intraday_collector import run_ingest_pipeline as collect_intraday

default_args = {
    "owner": "airflow",
    "retries": 5,                           # Thử lại 5 lần nếu lỗi
    "retry_delay": timedelta(minutes=1),     # Mỗi lần cách nhau 1 phút
    "depends_on_past": False,
}

with DAG(
    dag_id="stock_intraday_pipeline",
    default_args=default_args,
    #schedule="0 16,20,0,4,7 * * *",
    start_date=datetime(2025, 11, 12),
    catchup=False,
    tags=["stocks", "alphavantage", "bronze1", "bronze2"],
) as dag:


    bronze1_collect_task = PythonOperator(
        task_id="bronze1_intraday_collect_to_minio",
        python_callable=collect_intraday,
        op_kwargs={},
    )

    bronze2_convert_task = BashOperator(
        task_id="bronze2_convert_task",
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
            '--conf spark.sql.catalogImplementation=hive '
            '--conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 '
            '--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension '
            '--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog '
            '/opt/jobs/alphavantage/transform_into_delta.py"'
        ),
    )

    silver_transform_task = BashOperator(
        task_id="silver_transform_task",
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
            '--conf spark.sql.catalogImplementation=hive '
            '--conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 '
            '--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension '
            '--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog '
            '/opt/jobs/alphavantage/bronze2_to_silver.py"'
        ),
    )

    bronze1_collect_task >> bronze2_convert_task >> silver_transform_task
