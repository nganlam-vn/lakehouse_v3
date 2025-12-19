#stock_predict.py
import torch
import pandas as pd
import numpy as np
import io
import json
import duckdb 
from minio import Minio
from datetime import timedelta
import xgboost as xgb
import tempfile
import os
import joblib 
import sys

# === CẤU HÌNH ===
MINIO_CONFIG = { 
    "endpoint": "minio:9000", 
    "access_key": "admin", 
    "secret_key": "password", 
    "secure": False 
}
BUCKET = "models"
PREFIX = "hybrid/v2_tech_indicators/" 

# === HÀM TÍNH CHỈ BÁO ===
def compute_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def compute_macd(series, fast=12, slow=26, signal=9):
    exp1 = series.ewm(span=fast, adjust=False).mean()
    exp2 = series.ewm(span=slow, adjust=False).mean()
    macd = exp1 - exp2
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    return macd, signal_line

# === MODEL LSTM ===
class StockLSTM(torch.nn.Module):
    def __init__(self, input_size, hidden=64, layers=2):
        super().__init__()
        self.lstm = torch.nn.LSTM(input_size, hidden, layers, batch_first=True, dropout=0.2)
        self.fc = torch.nn.Linear(hidden, 1)
        self.sigmoid = torch.nn.Sigmoid()
        
    def forward(self, x):
        out, (hn, cn) = self.lstm(x)
        embedding = hn[-1, :, :] 
        return None, embedding

# === GLOBAL RESOURCES ===
_LSTM = None
_XGB = None
_SCALER = None
_META = None

def load_resources():
    global _LSTM, _XGB, _SCALER, _META
    if _LSTM and _XGB and _SCALER: return

    client = Minio(**MINIO_CONFIG)
    try:
        _META = json.loads(client.get_object(BUCKET, PREFIX + "metadata.json").read())
        
        scaler_bytes = client.get_object(BUCKET, PREFIX + "scaler.pkl").read()
        _SCALER = joblib.load(io.BytesIO(scaler_bytes))

        lstm_bytes = client.get_object(BUCKET, PREFIX + "lstm_extractor.pth").read()
        _LSTM = StockLSTM(input_size=len(_META['features']))
        _LSTM.load_state_dict(torch.load(io.BytesIO(lstm_bytes), map_location="cpu"))
        _LSTM.eval()
        
        xgb_bytes = client.get_object(BUCKET, PREFIX + "xgboost_cls.json").read()
        _XGB = xgb.XGBClassifier()
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp:
            tmp.write(xgb_bytes)
            tmp.close()
            _XGB.load_model(tmp.name)
            os.unlink(tmp.name)
            
    except Exception as e:
        print(f"❌ Lỗi tải resources: {e}")
        raise e

def predict_next_hours():
    load_resources()
    SEQ_LEN = _META['seq_len']
    FEATURES = _META['features']
    
    # 1. Load Data
    con = duckdb.connect(); con.sql("INSTALL httpfs; LOAD httpfs;")
    ep = MINIO_CONFIG['endpoint'].replace('http://', '').replace('https://', '')
    con.sql(f"SET s3_endpoint='{ep}'; SET s3_access_key_id='{MINIO_CONFIG['access_key']}'; SET s3_secret_access_key='{MINIO_CONFIG['secret_key']}'; SET s3_use_ssl=false; SET s3_url_style='path';")
    
    try:
        df = con.query(f"""
            SELECT symbol, timestamp, close, volume 
            FROM 's3://warehouse/silver/stocks_intraday/**/**/*.parquet' 
            WHERE timestamp >= CURRENT_DATE - INTERVAL '5 DAY' 
            ORDER BY symbol, timestamp
        """).df()
    finally: con.close()
    
    if df.empty: return None

    # 2. Feature Engineering
    df_processed_list = []
    for symbol, group in df.groupby('symbol'):
        # Sắp xếp nội bộ từng nhóm trước khi tính toán
        group = group.sort_values('timestamp')
        
        group['return'] = group['close'].pct_change()
        group['vol_change'] = group['volume'].pct_change().replace([np.inf, -np.inf], 0)
        group['rsi'] = compute_rsi(group['close'])
        group['macd'], group['macd_signal'] = compute_macd(group['close'])
        group = group.fillna(0)
        df_processed_list.append(group)
    
    df_final = pd.concat(df_processed_list)
    
    # === QUAN TRỌNG: SẮP XẾP LẠI TOÀN BỘ ===
    # Đảm bảo khi group by và tail() sẽ lấy đúng thời gian cuối cùng
    df_final = df_final.sort_values(['symbol', 'timestamp'])

    # 3. Predict
    result = {}
    for symbol, group in df_final.groupby('symbol'):
        # Lấy chuỗi dữ liệu mới nhất (Đuôi)
        sym_df = group.tail(SEQ_LEN)
        
        # Kiểm tra Debug thời gian
        last_ts = pd.to_datetime(sym_df['timestamp'].iloc[-1])
        # print(f"   🔎 {symbol}: Last Point @ {last_ts}") # Uncomment để debug nếu cần

        if len(sym_df) < SEQ_LEN:
            print(f"⚠️ Bỏ qua {symbol}: Thiếu data ({len(sym_df)}/{SEQ_LEN})")
            continue
            
        try:
            input_df = sym_df[FEATURES] 
            scaled_features = _SCALER.transform(input_df)
        except Exception as e:
            print(f"⚠️ Lỗi Scaling {symbol}: {e}")
            continue

        seq = torch.FloatTensor(scaled_features).unsqueeze(0) 
        with torch.no_grad():
            _, embedding = _LSTM(seq)
            embedding_numpy = embedding.numpy()

        probs = _XGB.predict_proba(embedding_numpy)[0]
        prob_up = probs[1]

        target_ts = last_ts + timedelta(hours=1)
        trend = "TĂNG 🟢" if prob_up > 0.5 else "GIẢM 🔴"
        
        result[symbol] = {
            "Time": last_ts.strftime('%H:%M'),
            "Target_1H": target_ts.strftime('%H:%M'),
            "Du_Bao": trend,
            "Xac_Suat": f"{round(prob_up * 100, 1)}%"
        }

    print(f" ({len(result)} mã):", json.dumps(result, indent=2, ensure_ascii=False))
    sys.stdout.flush()
    
    # Trả về None để Airflow không in dòng "Returned value was..."
    return None

if __name__ == "__main__":
    predict_next_hours()