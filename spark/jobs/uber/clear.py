import os
from minio import Minio
from trino.dbapi import connect

# --- CẤU HÌNH KẾT NỐI (Chạy từ máy Local) ---
MINIO_CONF = {
    "endpoint": "localhost:9000", 
    "access_key": "admin",
    "secret_key": "password",
    "secure": False,
    "bucket": "warehouse" 
}

TRINO_CONF = {
    "host": "localhost", 
    "port": 8082,        # Port 8082 map ra ngoài host (check docker-compose nếu khác)
    "user": "admin",
    "catalog": "delta",  
}

# --- DANH SÁCH MỤC TIÊU CẦN XÓA ---
# Lưu ý: Không có 'bronze1' trong này -> Dữ liệu gốc an toàn.
TARGETS = [
    # 1. BẢNG BRONZE 2 (Nơi bị lỗi Schema)
    {"type": "table", "schema": "bronze2", "table": "uber_bookings", "path": "bronze2/uber_bookings/"},
    
    # 2. BẢNG SILVER (Cần xóa để đồng bộ lại ID)
    {"type": "table", "schema": "silver",  "table": "uber_bookings", "path": "silver/uber_bookings/"},
    
    # 3. CÁC BẢNG GOLD (Xóa sạch để tính toán lại từ đầu)
    {"type": "table", "schema": "gold",    "table": "daily_kpi_vehicle",    "path": "gold/daily_kpi_vehicle/"},
    {"type": "table", "schema": "gold",    "table": "rush_hour_stats",      "path": "gold/rush_hour_stats/"},
    {"type": "table", "schema": "gold",    "table": "cancellation_reasons", "path": "gold/cancellation_reasons/"},
    {"type": "table", "schema": "gold",    "table": "payment_method_stats", "path": "gold/payment_method_stats/"},

    # 4. CHECKPOINTS (QUAN TRỌNG: Để Spark Streaming quên trạng thái cũ)
    # Xóa cái này thì Spark sẽ coi như chưa từng đọc file nào -> Đọc lại từ đầu
    {"type": "path_only", "path": "checkpoints/"} 
]

def clean_minio(client, bucket, prefix):
    """Xóa file vật lý trên MinIO"""
    print(f"   -> [MinIO] Scanning: {bucket}/{prefix}...")
    # List đệ quy tất cả objects
    objects = client.list_objects(bucket, prefix=prefix, recursive=True)
    
    # Gom vào list để xóa
    delete_list = [obj.object_name for obj in objects]
    
    if delete_list:
        # Xóa từng batch (MinIO hỗ trợ xóa nhiều obj 1 lúc nhưng library python này loop an toàn hơn)
        for obj_name in delete_list:
            client.remove_object(bucket, obj_name)
        print(f"      Đã xóa {len(delete_list)} files/objects.")
    else:
        print("      Không tìm thấy file nào (Sạch).")

def clean_trino(conn, schema, table):
    """Xóa bảng Metadata trên Trino"""
    cur = conn.cursor()
    full_name = f"{schema}.{table}"
    print(f"   -> [Trino] Dropping table: {full_name}...")
    try:
        cur.execute(f"DROP TABLE IF EXISTS {full_name}")
        print("      Drop thành công (hoặc bảng chưa tồn tại).")
    except Exception as e:
        print(f"      Lỗi Trino (Có thể bỏ qua nếu bảng chưa tạo): {e}")

def main():
    print("=======================================================")
    print("   RESET LAKEHOUSE TOOL - CLEAN MODE")
    print("   Targets: Bronze2, Silver, Gold, Checkpoints")
    print("   SAFE ZONE: Bronze1 (Raw Data) will be KEPT.")
    print("=======================================================")
    
    # 1. Kết nối
    try:
        minio_client = Minio(
            MINIO_CONF["endpoint"],
            access_key=MINIO_CONF["access_key"],
            secret_key=MINIO_CONF["secret_key"],
            secure=MINIO_CONF["secure"]
        )
        
        trino_conn = connect(
            host=TRINO_CONF["host"],
            port=TRINO_CONF["port"],
            user=TRINO_CONF["user"],
            catalog=TRINO_CONF["catalog"]
        )
    except Exception as e:
        print(f"Lỗi kết nối (Check lại Port/Docker): {e}")
        return

    # 2. Duyệt qua từng mục tiêu
    for t in TARGETS:
        print(f"\n--- Xử lý mục tiêu: {t.get('path')} ---")
        
        # Nếu là Bảng -> Xóa Metadata trên Trino trước
        if t["type"] == "table":
            clean_trino(trino_conn, t["schema"], t["table"])
        
        # Xóa File vật lý trên MinIO (Cả Table và Checkpoint đều nằm trên MinIO)
        clean_minio(minio_client, MINIO_CONF["bucket"], t["path"])

    print("\n=======================================================")
    print("   HOÀN TẤT! HỆ THỐNG ĐÃ VỀ TRẠNG THÁI SẠCH SẼ.")
    print("   Bạn có thể Trigger lại DAG ngay bây giờ.")
    print("=======================================================")

if __name__ == "__main__":
    main()