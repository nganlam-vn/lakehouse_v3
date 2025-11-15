# -*- coding: utf-8 -*-
import os, json, time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple
import requests
from minio import Minio
from io import BytesIO

# ====== CẤU HÌNH ======
# API key: ưu tiên lấy từ ENV NEWSAPI_KEY; fallback hardcode (nếu muốn)
API_KEY = os.getenv("NEWSAPI_KEY", "6ca73a4cd9e448dd80fdff8999905c3e")
KEYWORDS_LIST = ['"stock market"', 'inflation', '"Federal Reserve"', 'Bitcoin', 'cryptocurrency']
KEYWORDS = " OR ".join(KEYWORDS_LIST)
LANGUAGE = "en"
PAGE_SIZE = 100
MAX_PAGES_PER_DAY = 10
REQUEST_TIMEOUT = (10, 30)
SLEEP_BETWEEN_REQ = 1.0

# MinIO (hardcode theo stack của bạn)
MINIO_ENDPOINT = "minio:9000"
MINIO_SECURE = False
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
MINIO_BUCKET = "warehouse"
BRONZE1_PREFIX = "bronze1/finance_news" 

VN_TZ = timezone(timedelta(hours=7))

# ====== HELPERS ======
def iso_utc_range_for_local_date_yyyy_mm_dd(date_str: str) -> Tuple[str, str]:
    d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=VN_TZ)
    start_local = d.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local   = d.replace(hour=23, minute=59, second=59, microsecond=999000)
    start_utc = start_local.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    end_utc   = end_local.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return start_utc, end_utc

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "news-collector/1.0"})
    return s

def safe_get(session: requests.Session, url: str, params: dict):
    for attempt in range(5):
        try:
            r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code in (429, 500, 502, 503, 504):
                wait = min(2 ** attempt, 30)
                print(f"   ↪ HTTP {r.status_code}, retry sau {wait}s…")
                time.sleep(wait)
                continue
            return r
        except requests.RequestException as e:
            wait = min(2 ** attempt, 30)
            print(f"   ↪ Exception {e}, retry sau {wait}s…")
            time.sleep(wait)
    return None

def fetch_one_day(session: requests.Session, date_str: str) -> List[Dict]:
    base_url = "https://newsapi.org/v2/everything"
    f_iso, t_iso = iso_utc_range_for_local_date_yyyy_mm_dd(date_str)

    all_items, seen_urls = [], set()
    print(f"--> {date_str} (VN time) | from={f_iso} to={t_iso}")

    for page in range(1, MAX_PAGES_PER_DAY + 1):
        params = {
            "q": KEYWORDS,
            "from": f_iso,
            "to": t_iso,
            "language": LANGUAGE,
            "sortBy": "publishedAt",
            "pageSize": PAGE_SIZE,
            "page": page,
            "apiKey": API_KEY,
        }
        r = safe_get(session, base_url, params)
        if r is None:
            print("   ✗ Bỏ qua do lỗi mạng sau nhiều lần thử")
            break

        try:
            data = r.json()
        except Exception:
            print("   ✗ Không parse được JSON")
            break

        if r.status_code != 200 or data.get("status") != "ok":
            print(f"   ✗ Lỗi API: {data.get('message') or r.text}")
            break

        items = data.get("articles", []) or []
        if not items:
            if page == 1:
                print("   • 0 bài")
            break

        added = 0
        for it in items:
            url = (it or {}).get("url")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            all_items.append(it)
            added += 1

        total = data.get("totalResults")
        print(f"   • Trang {page}: nhận {len(items)} bài, thêm mới {added}, totalResults={total}")

        if len(items) < PAGE_SIZE:
            break

        time.sleep(SLEEP_BETWEEN_REQ)

    return all_items

def _ensure_bucket(minio_client: Minio, name: str):
    if not minio_client.bucket_exists(name):
        minio_client.make_bucket(name)

def upload_bytes_to_minio(data: bytes, key: str):
    client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    _ensure_bucket(client, MINIO_BUCKET)
    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=key,
        data=BytesIO(data),
        length=len(data),
        content_type="application/json",
    )
    print(f"⬆️  Uploaded: s3://{MINIO_BUCKET}/{key} ({len(data)} bytes)")

# ====== ENTRYPOINT cho Airflow (PythonOperator sẽ gọi hàm này) ======
def collect_news_to_bronze1(days_back: int = 5) -> int:
    if not API_KEY:
        raise RuntimeError("NEWSAPI_KEY is not set")

    sess = make_session()
    today = datetime.now(VN_TZ)
    date_list = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, days_back + 1)]
    print(f"Sẽ lấy tin cho các ngày (trừ hôm nay {today.strftime('%Y-%m-%d')}): {date_list}\n")

    total = 0
    for date_str in date_list:
        items = fetch_one_day(sess, date_str)
        total += len(items)

        # Ghi từng ngày thành một object JSON (list)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        key = f"{BRONZE1_PREFIX}/date={date_str}/ts_{ts}.json"
        payload = json.dumps(items, ensure_ascii=False, indent=2).encode("utf-8")
        upload_bytes_to_minio(payload, key)

        time.sleep(SLEEP_BETWEEN_REQ)

    print(f"✅ Done. Tổng số bài: {total}")
    return total
