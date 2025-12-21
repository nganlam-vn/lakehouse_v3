from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from tasks.ingestion.coin_collector import main as collect_coin_data
from airflow.providers.standard.operators.python import PythonOperator

# timezone theo máy bạn (HCM)
TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="official_coin_to_delta_dag",
    description="Run a simple Delta Lake job in delta-spark container (local Spark)",
    start_date=datetime(2024, 1, 1, tzinfo=TZ),
    schedule=None,              # chạy thủ công; muốn cron thì sửa thành "0 * * * *" v.v.
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "coinmarketcap", "delta"],
) as dag:

    collect_data_task = PythonOperator(
        task_id='collect_coin_data',
        python_callable=collect_coin_data,

    )
    run_delta_job = BashOperator(
        task_id="run_delta_job",
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
            '/opt/jobs/coin_to_delta_w_cp.py"'
        ),
    )

    collect_data_task >> run_delta_job
