# tasks/ingestion/fetch_finance_news.py
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple
import os, json, time

# Keep only light-weight constants at module level
VN_TZ = timezone(timedelta(hours=7))
API_KEY = "6ca73a4cd9e448dd80fdff8999905c3e"

SYMBOL_QUERIES = {
    "AAPL": '"Apple" OR "AAPL"',
    "MSFT": '"Microsoft" OR "MSFT"',
    "META": '"Meta Platforms" OR "META" OR "Facebook" OR "Mark Zuckerberg"',
    "GOOGL": '"Google" OR "Alphabet" OR "GOOGL"',
    "AMZN": '"Amazon" OR "AMZN" OR "AWS"',
}

IMPACT_CONTEXT_QUERY = (
    '"earnings" OR "revenue" OR "profit" OR "loss" OR "forecast" OR '
    '"stock" OR "shares" OR "dividend" OR "buyback" OR "analyst rating" OR '
    '"acquisition" OR "merger" OR "partnership" OR "layoffs" OR "restructuring" OR '
    '"lawsuit" OR "sued" OR "investigation" OR "SEC" OR "FTC" OR '
    '"antitrust" OR "regulatory" OR "security breach" OR "outage" OR '
    '"product launch" OR "unveils" OR "recalls" OR '
    '"CEO" OR "CFO" OR "CTO"'
)

LANGUAGE = "en"
PAGE_SIZE = 100
MAX_PAGES_PER_SYMBOL = 5
REQUEST_TIMEOUT = (10, 30)
SLEEP_BETWEEN_REQ = 1.0

MINIO_ENDPOINT = "minio:9000"
MINIO_SECURE = False
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
MINIO_BUCKET = "warehouse"
BRONZE1_PREFIX = "bronze1/finance_news"

# Note: heavy imports moved inside functions below


def make_session() -> "requests.Session":
    import requests  # lazy import
    s = requests.Session()
    s.headers.update({"User-Agent": "news-collector/1.0"})
    return s


def safe_get(session, url: str, params: dict):
    import time
    import requests  # lazy import to ensure module loaded at runtime
    for attempt in range(5):
        try:
            r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code in (429, 500, 502, 503, 504):
                wait = min(2 ** attempt, 30)
                time.sleep(wait)
                continue
            return r
        except Exception:
            wait = min(2 ** attempt, 30)
            time.sleep(wait)
    return None


def iso_utc_range_for_last_hours(hours: int) -> Tuple[str, str, datetime, datetime]:
    now_local = datetime.now(VN_TZ)
    start_local = now_local - timedelta(hours=hours)
    now_utc = now_local.astimezone(timezone.utc)
    start_utc = start_local.astimezone(timezone.utc)
    from_utc_str = start_utc.strftime("%Y-%m-%dT%H:%M:%S")
    to_utc_str = now_utc.strftime("%Y-%m-%dT%H:%M:%S")
    return from_utc_str, to_utc_str, start_local, now_local


def fetch_last_hours_for_symbol(session, symbol: str, hours: int = 4):
    import time
    base_url = "https://newsapi.org/v2/everything"
    f_iso, t_iso, start_local, end_local = iso_utc_range_for_last_hours(hours)
    base_query = SYMBOL_QUERIES[symbol]
    final_query = f"({base_query}) AND ({IMPACT_CONTEXT_QUERY})"
    if len(final_query) > 500:
        return [], start_local, end_local
    all_items, seen_urls = [], set()
    for page in range(1, MAX_PAGES_PER_SYMBOL + 1):
        params = {
            "q": final_query,
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
            break
        try:
            data = r.json()
        except Exception:
            break
        if r.status_code != 200 or data.get("status") != "ok":
            break
        items = data.get("articles", []) or []
        if not items:
            break
        for it in items:
            url = it.get("url")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            enriched = dict(it)
            enriched["symbol"] = symbol
            all_items.append(enriched)
        if len(items) < PAGE_SIZE:
            break
        time.sleep(SLEEP_BETWEEN_REQ)
    return all_items, start_local, end_local


def _ensure_bucket(minio_client, name: str):
    if not minio_client.bucket_exists(name):
        minio_client.make_bucket(name)


def upload_bytes_to_minio(data: bytes, key: str):
    # lazy import Minio client
    from minio import Minio
    from io import BytesIO
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


def collect_news_last_hours(window_hours: int = 4, symbols: List[str] = None) -> int:
    # lazy import requests session creation inside
    sess = make_session()
    grand_total = 0
    for symbol in symbols or list(SYMBOL_QUERIES.keys()):
        if symbol not in SYMBOL_QUERIES:
            continue
        items, start_local, end_local = fetch_last_hours_for_symbol(sess, symbol, window_hours)
        total = len(items)
        grand_total += total
        if total > 0:
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            date_str = start_local.strftime("%Y-%m-%d")
            from_h = start_local.strftime("%H%M")
            to_h = end_local.strftime("%H%M")
            key = (
                f"{BRONZE1_PREFIX}/date={date_str}/symbol={symbol}/"
                f"from_{from_h}_to_{to_h}_ts_{ts}.json"
            )
            payload = json.dumps(items, ensure_ascii=False, indent=2).encode("utf-8")
            upload_bytes_to_minio(payload, key)
        print(f"Symbol {symbol}: {total} bài")
        time.sleep(SLEEP_BETWEEN_REQ)
    print(f"Tổng số bài: {grand_total}")
    return grand_total
