from datetime import datetime, timezone
from urllib.parse import urlparse
import configparser

from minio import Minio
import pandas as pd
from minio.error import S3Error
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects, HTTPError
# from pyspark.sql import functions as F

config = configparser.ConfigParser()
config.read('/opt/airflow/config/config.ini')

# Initialize MinIO client from config
minio_client = Minio(
    endpoint=config.get('minio', 'endpoint'),
    access_key=config.get('minio', 'access_key'),
    secret_key=config.get('minio', 'secret_key'),
    secure=config.getboolean('minio', 'secure')
)

# CoinMarketCap configuration
MC_API_KEY = config.get('coinmarketcap', 'api_key')
CMC_BASE_URL = config.get('coinmarketcap', 'base_url')

def upload_json_bytes(data:bytes, object_name:str, bucket_name:str=None):
    if bucket_name is None:
        bucket_name = config.get('minio', 'bucket')
    """Ingest data into MinIO bucket"""
    # Ensure the bucket exists
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
    
    # Upload the data
    from io import BytesIO
    data_stream = BytesIO(data)
    data_size = len(data)
    
    minio_client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=data_stream,
        length=data_size,
        content_type='application/json'
    )
    print(f"Data ingested to {bucket_name}/{object_name}")


def fetch_coinmarketcap(limit: int) -> pd.DataFrame:
    """Gọi API CMC và trả về DataFrame"""
    url = CMC_BASE_URL
    params = {"start": "1", "limit": str(limit), "convert": "USD"}
    headers = {
        "Accepts": "application/json",
        "X-CMC_PRO_API_KEY": MC_API_KEY
    }

    sess = Session()
    sess.headers.update(headers)
    try:
        resp = sess.get(url, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        if "data" not in payload:
            raise ValueError(f"Unexpected payload: keys={list(payload.keys())}")
        df = pd.json_normalize(payload["data"])
        df["dt_utc_record_to_bronze1"] = pd.Timestamp.utcnow()
        df["cd_bronze1_id"] = range(len(df))
        return df
    except (ConnectionError, Timeout, TooManyRedirects, HTTPError) as e:
        raise RuntimeError(f"CMC request failed: {e}")

def main():
    # 1) Lấy dữ liệu
    df = fetch_coinmarketcap(limit=500)

    # 2) Serialize JSON (records) → bytes
    data_bytes = df.to_json(orient="records", date_format="iso").encode("utf-8")

    # 3) Định danh file theo ngày/giờ, prefix bronze
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"bronze1/coinmarketcap/ts_{ts}.json"

    # 4) Upload vào bucket 'warehouse'
    upload_json_bytes(data_bytes, object_name=key, bucket_name="warehouse")

if __name__ == "__main__":
    try:
        main()
    except S3Error as e:
        print("MinIO error:", e)
    except Exception as ex:
        print("Error:", ex)