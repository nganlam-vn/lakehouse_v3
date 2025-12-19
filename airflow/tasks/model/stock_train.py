# stock_train.py
from typing import Optional

def train_model():
    # === 1. LAZY IMPORT ===
    import pandas as pd
    import numpy as np
    import torch
    import torch.nn as nn
    from torch.utils.data import DataLoader, TensorDataset
    
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import accuracy_score, f1_score, classification_report
    import xgboost as xgb
    
    import joblib
    import json
    from datetime import datetime
    from minio import Minio
    import io
    import duckdb 
    import tempfile
    import os
    import sys
    import copy


    # === CẤU HÌNH ===
    MINIO_CONFIG = {
        "endpoint": "minio:9000",
        "access_key": "admin",
        "secret_key": "password",
        "secure": False
    }
    BUCKET = "models"
    PREFIX = "hybrid/v2_tech_indicators/" 
    
    # --- CẤU HÌNH EPOCH ---
    MAX_EPOCHS = 50
    
    SEQ_LEN = 120    
    LOOK_AHEAD = 60 
    FEATURES = ['return', 'vol_change', 'rsi', 'macd', 'macd_signal'] 

    client = Minio(**MINIO_CONFIG)
    if not client.bucket_exists(BUCKET): client.make_bucket(BUCKET)

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

    def upload_bytes(data: bytes, name: str):
        client.put_object(BUCKET, name, io.BytesIO(data), len(data))
        print(f"Uploaded: {name}")

    # === MODEL LSTM ===
    class StockLSTM(nn.Module):
        def __init__(self, input_size=len(FEATURES), hidden=64, layers=2):
            super().__init__()
            self.lstm = nn.LSTM(input_size, hidden, layers, batch_first=True, dropout=0.2)
            self.fc = nn.Linear(hidden, 1)
            self.sigmoid = nn.Sigmoid()

        def forward(self, x):
            out, (hn, cn) = self.lstm(x)
            embedding = hn[-1, :, :] 
            prediction = self.sigmoid(self.fc(out[:, -1, :]))
            return prediction, embedding

    # === 2. LOAD DATA ===
    con = duckdb.connect(); con.sql("INSTALL httpfs; LOAD httpfs;")
    ep = MINIO_CONFIG['endpoint'].replace('http://', '').replace('https://', '')
    con.sql(f"SET s3_endpoint='{ep}'; SET s3_access_key_id='{MINIO_CONFIG['access_key']}'; SET s3_secret_access_key='{MINIO_CONFIG['secret_key']}'; SET s3_use_ssl=false; SET s3_url_style='path';")

    try:
        df = con.query("SELECT symbol, timestamp, close, volume FROM 's3://warehouse/silver/stocks_intraday/**/**/*.parquet' ORDER BY symbol, timestamp").df()
    except Exception as e: print(f"Err: {e}"); return
    finally: con.close()

    if df.empty: raise ValueError("No Data found.")

    # === 3. FEATURE ENGINEERING ===
    df_list = []
    for symbol, group in df.groupby('symbol'):
        group = group.sort_values('timestamp')
        group['return'] = group['close'].pct_change()
        group['vol_change'] = group['volume'].pct_change().replace([np.inf, -np.inf], 0)
        group['rsi'] = compute_rsi(group['close'])
        group['macd'], group['macd_signal'] = compute_macd(group['close'])
        group['future_close'] = group['close'].shift(-LOOK_AHEAD)
        group['target'] = (group['future_close'] > group['close']).astype(int)
        group = group.dropna().reset_index(drop=True)
        df_list.append(group)

    if not df_list: raise ValueError("Không đủ dữ liệu.")
    df_train = pd.concat(df_list)
    
    scaler = StandardScaler()
    df_train[FEATURES] = scaler.fit_transform(df_train[FEATURES])

    # === 4. SEQUENCE GENERATION ===
    X, y = [], []
    for _, group in df_train.groupby('symbol'):
        feat_data = group[FEATURES].values
        target_data = group['target'].values
        if len(feat_data) < SEQ_LEN: continue
        stride = 2 
        for i in range(SEQ_LEN, len(feat_data) - LOOK_AHEAD + 1, stride):
            X.append(feat_data[i-SEQ_LEN:i])
            y.append(target_data[i + LOOK_AHEAD - 1]) 
    
    X, y = np.array(X), np.array(y)
    print(f"Số mẫu huấn luyện: {len(X)}")
    
    split = int(0.8 * len(X))
    train_X, val_X = torch.FloatTensor(X[:split]), torch.FloatTensor(X[split:])
    train_y, val_y = torch.FloatTensor(y[:split]), torch.FloatTensor(y[split:])
    
    train_loader = DataLoader(TensorDataset(train_X, train_y.unsqueeze(1)), batch_size=64, shuffle=True)

    # === 5. TRAIN LSTM  ===
    model_lstm = StockLSTM()
    opt = torch.optim.Adam(model_lstm.parameters(), lr=0.001)
    crit = nn.BCELoss()

    best_val_loss = float('inf')
    best_model_state = None 

    for epoch in range(MAX_EPOCHS): 
        model_lstm.train()
        total_loss = 0
        for xb, yb in train_loader:
            opt.zero_grad()
            pred, _ = model_lstm(xb)
            loss = crit(pred, yb)
            loss.backward()
            opt.step()
            total_loss += loss.item()
        
        avg_train_loss = total_loss / len(train_loader)

        # Validate
        model_lstm.eval()
        with torch.no_grad():
            val_pred, _ = model_lstm(val_X)
            val_loss = crit(val_pred, val_y.unsqueeze(1)).item()

        if epoch % 10 == 0: 
            print(f"   Epoch {epoch:3d} | Train Loss: {avg_train_loss:.4f} | Val Loss: {val_loss:.4f}")

        if val_loss < best_val_loss:
            best_val_loss = val_loss
            best_model_state = copy.deepcopy(model_lstm.state_dict())
            
    # Load lại model tốt nhất để dùng cho XGBoost
    if best_model_state:
        model_lstm.load_state_dict(best_model_state)
        print(f"✅ Đã hoàn thành.(Val Loss: {best_val_loss:.4f})")

    # === 6. TRAIN XGBOOST ===
    model_lstm.eval()
    with torch.no_grad():
        _, train_emb = model_lstm(train_X)
        _, val_emb = model_lstm(val_X)
        X_train_xgb = train_emb.numpy()
        y_train_xgb = train_y.numpy()
        X_val_xgb = val_emb.numpy()
        y_val_xgb = val_y.numpy()

    xgb_model = xgb.XGBClassifier(
        n_estimators=200,
        learning_rate=0.05,
        max_depth=9,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric="logloss"
    )
    
    xgb_model.fit(X_train_xgb, y_train_xgb, eval_set=[(X_val_xgb, y_val_xgb)], verbose=False)
    
    # === 7. ĐÁNH GIÁ ===
    print("\n" + "="*50)
    print("KẾT QUẢ ĐÁNH GIÁ (Validation Set)")
    print("="*50)
    
    y_pred = xgb_model.predict(X_val_xgb)
    acc = accuracy_score(y_val_xgb, y_pred)
    f1 = f1_score(y_val_xgb, y_pred)
    
    print(f"Accuracy Tổng : {acc:.2%}")
    print(f"F1-Score Tổng : {f1:.2%}")
    print("-" * 50)

    # === 8. UPLOAD ===
    buf = io.BytesIO()
    torch.save(model_lstm.state_dict(), buf)
    upload_bytes(buf.getvalue(), PREFIX + "lstm_extractor.pth")
    
    with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp:
        xgb_model.save_model(tmp.name)
        tmp.close()
        with open(tmp.name, "rb") as f: upload_bytes(f.read(), PREFIX + "xgboost_cls.json")
        os.unlink(tmp.name)

    buf = io.BytesIO()
    joblib.dump(scaler, buf)
    upload_bytes(buf.getvalue(), PREFIX + "scaler.pkl")

    meta = {"seq_len": SEQ_LEN, "features": FEATURES, "look_ahead": LOOK_AHEAD, "type": "hybrid_v2_opt"}
    upload_bytes(json.dumps(meta).encode(), PREFIX + "metadata.json")
    
    print("✅ Train Hoàn Tất!")

if __name__ == "__main__":
    train_model()