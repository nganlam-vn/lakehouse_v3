from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

# Timezone (Việt Nam)
TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,                          
    "retry_delay": timedelta(minutes=1),        
}

with DAG(
    dag_id="coinmarketcap_official_pipeline",
    description="CoinMarketCap -> Bronze1 JSON -> Bronze2 Delta -> Silver",
    start_date=pendulum.datetime(2024, 1, 1, tz=TZ),
    #schedule="*/5 * * * *",                     # chạy mỗi 5 phút
    catchup=False,
    max_active_runs=1,                          # không cho chạy chồng
    default_args=DEFAULT_ARGS,
    tags=["coin", "delta", "spark", "minio"],
) as dag:

    # Lazy import collector để tránh timeout khi load DAG
    def _collect_coin_data():
        from tasks.ingestion.coin_collector import main as _main
        _main()

    collect_data_task = PythonOperator(
        task_id="collect_coin_data",
        python_callable=_collect_coin_data,
    )

    run_delta_job = BashOperator(
        task_id="bronze1_to_bronze2",
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
            '/opt/jobs/coin/convert_to_delta.py"'
        ),
    )

    silver_convert_task = BashOperator(
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
            '/opt/jobs/coin/coin_bronze2_to_silver.py"'
        ),
    )

    collect_data_task >> run_delta_job >> silver_convert_task
