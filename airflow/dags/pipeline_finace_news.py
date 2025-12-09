# finance_news_pipeline.py
# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 10,
    "retry_delay": timedelta(minutes=1),
}

def _collect_finance_news(**context):
    from tasks.ingestion.fetch_finance_news import collect_news_last_hours
    collect_news_last_hours(window_hours=28)


with DAG(
    dag_id="newsapi_finance_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1, tzinfo=TZ),
    # schedule="0 */4 * * *",
    catchup=False,
    tags=["newsapi", "bronze1", "bronze2", "finance"],
    max_active_runs=1,
) as dag:

    collect_news_task = PythonOperator(
        task_id="collect_finance_news",
        python_callable=_collect_finance_news,
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
            '/opt/jobs/finance_news/bronze1_to_bronze2.py"'
        ),
    )

    transform_to_silver = BashOperator(
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
            '/opt/jobs/finance_news/finance_bronze2_to_silver.py"'
        ),
    )

    collect_news_task >> bronze2_convert_task >> transform_to_silver
