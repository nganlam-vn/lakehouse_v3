# /opt/jobs/alphavantage/stock_intraday_collector.py
from datetime import datetime, timezone
from io import BytesIO
import json
import time
import random

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from zoneinfo import ZoneInfo

from minio import Minio
from minio.error import S3Error

# ========= CẤU HÌNH =========
ALPHA_VANTAGE_API_KEY = "UIHZYFBBM6NVWK4Z"

SYMBOLS = ["AAPL", "MSFT", "IBM", "GOOGL", "AMZN"]
INTERVAL = "5min"               # 1min, 5min, 15min, 30min, 60min
OUTPUTSIZE = "compact"          # compact = 100 điểm gần nhất
ADJUSTED = "true"
EXTENDED_HOURS = "true"

MINIO_ENDPOINT = "minio:9000"
MINIO_SECURE = False
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
MINIO_BUCKET = "warehouse"

BRONZE1_PREFIX = "bronze1/stocks_intraday"

BATCH_SIZE = 2
SLEEP_BETWEEN_SYMBOLS_SEC = 20
SLEEP_BETWEEN_BATCHES_SEC = 70


# ========= HTTP SESSION VỚI RETRY =========
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=3,
        read=3,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


session = make_session()

# ========= MINIO CLIENT =========
minio_client = Minio(
    endpoint=MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE,
)


def _ensure_bucket(name: str):
    if not minio_client.bucket_exists(name):
        minio_client.make_bucket(name)


def _upload_bytes(data: bytes, bucket: str, key: str):
    _ensure_bucket(bucket)
    minio_client.put_object(
        bucket_name=bucket,
        object_name=key,
        data=BytesIO(data),
        length=len(data),
        content_type="application/json",
    )
    print(f"uploaded s3://{bucket}/{key} ({len(data)} bytes)")


def _parse_ts_to_ny_date(ts_str: str):
    ny = ZoneInfo("America/New_York")
    dt_naive = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    return dt_naive.replace(tzinfo=ny).date()


# ========= GỌI API & LỌC DỮ LIỆU =========
def fetch_intraday_symbol(symbol: str, interval: str, target_date_ny=None) -> list[dict]:
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": interval,
        "outputsize": OUTPUTSIZE,
        "adjusted": ADJUSTED,
        "extended_hours": EXTENDED_HOURS,
        "datatype": "json",
        "apikey": ALPHA_VANTAGE_API_KEY,
    }

    try:
        r = session.get("https://www.alphavantage.co/query", params=params, timeout=(10, 45))
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"{symbol}: request error → {e}")
        return []

    key = f"Time Series ({interval})"
    if key not in data:
        print(f"{symbol}: no data ({data})")
        return []

    series = data[key]

    # Nếu không truyền ngày → lấy ngày giao dịch mới nhất trong payload
    if target_date_ny is None:
        all_dates = {_parse_ts_to_ny_date(ts) for ts in series.keys()}
        if not all_dates:
            print(f"{symbol}: no timestamp in payload")
            return []
        target_date_ny = max(all_dates)
        print(f"{symbol}: infer trading date = {target_date_ny}")

    recs = []
    for ts, ohlc in series.items():
        if _parse_ts_to_ny_date(ts) != target_date_ny:
            continue
        recs.append(
            {
                "timestamp": ts,
                "open": float(ohlc["1. open"]),
                "high": float(ohlc["2. high"]),
                "low": float(ohlc["3. low"]),
                "close": float(ohlc["4. close"]),
                "volume": int(ohlc["5. volume"]),
                "symbol": symbol,
                "interval": interval,
                "tz": "America/New_York",
                "date_ny": str(target_date_ny),
                "dt_utc_record_to_bronze1": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        )

    recs.sort(key=lambda x: x["timestamp"])
    print(f"{symbol}: fetched {len(recs)} records for {target_date_ny}")
    return recs


# ========= GHI BRONZE1 =========
def upload_symbol_records_as_jsonl(symbol: str, recs: list[dict], target_date_ny):
    if not recs:
        print(f"{symbol}: 0 record → skip")
        return

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{BRONZE1_PREFIX}/date_ny={target_date_ny}/symbol={symbol}/ts_{run_ts}.json"

    payload = ("\n".join(json.dumps(r) for r in recs) + "\n").encode("utf-8")

    _upload_bytes(payload, bucket=MINIO_BUCKET, key=key)
    print(f"{symbol}: uploaded {len(recs)} recs")


# ========= MAIN =========
def main():
    batches = [SYMBOLS[i:i+BATCH_SIZE] for i in range(0, len(SYMBOLS), BATCH_SIZE)]
    print(f"Collector start | interval={INTERVAL} | batches={len(batches)}")

    for idx, batch in enumerate(batches, start=1):
        print(f"Batch {idx}/{len(batches)}: {batch}")

        for sym in batch:
            recs = fetch_intraday_symbol(sym, INTERVAL, target_date_ny=None)
            target_date_ny = recs[0]["date_ny"] if recs else "unknown"
            upload_symbol_records_as_jsonl(sym, recs, target_date_ny)
            time.sleep(SLEEP_BETWEEN_SYMBOLS_SEC + random.randint(1, 3))

        if idx < len(batches):
            time.sleep(SLEEP_BETWEEN_BATCHES_SEC)


if __name__ == "__main__":
    try:
        main()
    except S3Error as e:
        print("MinIO error:", e)
    except Exception as ex:
        print("Error:", ex)
