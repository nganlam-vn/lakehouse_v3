from datetime import datetime
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# timezone theo máy bạn (HCM)
TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="delta_job_dag_2",
    description="Run a simple Delta Lake job in delta-spark container (local Spark)",
    start_date=datetime(2024, 1, 1, tzinfo=TZ),
    schedule=None,              # chạy thủ công; muốn cron thì sửa thành "0 * * * *" v.v.
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["delta", "spark", "minio"],
) as dag:

    # 1) Kiểm tra container delta-spark và phiên bản spark
    spark_version = BashOperator(
        task_id="spark_version",
        bash_command=(
            'docker exec delta-spark bash -lc "python3 -c \\"import pyspark; '
            'print(pyspark.__version__)\\" && pyspark --version"'
        ),
    )

    # 2) Chạy script PySpark ghi/đọc Delta trên MinIO
    #    Lưu ý: script nằm ở ./spark/jobs/delta_quickstart.py và đã được mount vào /opt/jobs
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
            '--conf spark.sql.catalogImplementation=hive '
            '--conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 '
            '--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension '
            '--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog '
            '/opt/jobs/delta_quickstart.py"'
        ),
    )

    spark_version >> run_delta_job
