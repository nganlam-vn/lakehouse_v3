import pandas as pd
import requests
import io
from minio import Minio
from datetime import datetime
import logging

# === 1. CẤU HÌNH ===
ALPHA_CONFIG = {
    "api_key": "R6UUYGFK7M36GUQG", 
    "symbol": "AAPL",             
    "interval": "1min",
    "outputsize": "compact"       
}

MINIO_CONFIG = {
    "endpoint": "minio:9000", 
    "access_key": "admin",
    "secret_key": "password",
    "bucket_name": "warehouse",   
    "secure": False
}

# === 2. HÀM LOGIC (Internal) ===
def _fetch_data():
    logging.info(f"🔄 Fetching {ALPHA_CONFIG['symbol']}...")
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": ALPHA_CONFIG['symbol'],
        "interval": ALPHA_CONFIG['interval'],
        "outputsize": ALPHA_CONFIG['outputsize'],
        "datatype": "csv",
        "apikey": ALPHA_CONFIG['api_key']
    }
    
    try:
        response = requests.get(url, params=params)
        df = pd.read_csv(io.StringIO(response.text))
        
        # Xử lý data
        df = df.sort_values("timestamp").reset_index(drop=True)
        df['symbol'] = ALPHA_CONFIG['symbol']
        # df['source'] = 'alphavantage.co'
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        return df
    except Exception as e:
        logging.error(f"❌ Fetch Error: {e}")
        return None

def _upload_hive_style(df):
    """Hàm nội bộ: Chỉ chịu trách nhiệm upload DataFrame có sẵn"""
    client = Minio(**{k: v for k, v in MINIO_CONFIG.items() if k != 'bucket_name'})
    
    if not client.bucket_exists(MINIO_CONFIG["bucket_name"]):
        client.make_bucket(MINIO_CONFIG["bucket_name"])

    # Chuẩn bị path
    current_date = datetime.now().strftime('%Y-%m-%d')
    file_name = f"data_{datetime.now().strftime('%H%M%S')}.json"
    
    object_name = (
        f"bronze1/stocks_intraday/"
        f"date_ny={current_date}/"
        f"symbol={ALPHA_CONFIG['symbol']}/"
        f"{file_name}"
    )
    
    # Convert JSON
    buf = io.BytesIO()
    df.to_json(buf, orient='records', date_format='iso')
    data = buf.getvalue()
    
    # Upload
    client.put_object(
        MINIO_CONFIG["bucket_name"],
        object_name,
        io.BytesIO(data),
        len(data),
        content_type="application/json"
    )
    logging.info(f"✅ Uploaded to: {MINIO_CONFIG['bucket_name']}/{object_name}")

# === 3. HÀM CHÍNH CHO AIRFLOW GỌI (MAIN ENTRY POINT) ===
def run_ingest_pipeline():
    logging.info("🚀 Bắt đầu Task Ingest Data...")
    
    # Bước 1: Lấy dữ liệu
    df = _fetch_data()
    
    # Bước 2: Kiểm tra và Upload
    if df is not None and not df.empty:
        _upload_hive_style(df)
    else:
        logging.warning("⚠️ Không có dữ liệu để upload hoặc gặp lỗi khi fetch.")