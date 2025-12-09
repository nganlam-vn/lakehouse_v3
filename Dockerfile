FROM apache/airflow:3.1.0
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
RUN pip install --no-cache-dir tensorflow
