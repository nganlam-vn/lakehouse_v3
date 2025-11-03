from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("delta_quickstart")
    .getOrCreate()
)

# Ghi/đọc Delta trên MinIO (đường dẫn S3A)
path = "s3a://warehouse/bronze/delta_quickstart"

# Ghi (overwrite) một DataFrame nhỏ
# Append 10 rows to the existing Delta location
spark.range(0, 10).write.format("delta").mode("append").save(path)

# Đọc lại và show
df = spark.read.format("delta").load(path)
df.show()

# (Optional) Đăng ký bảng Hive Metastore để Trino/Hive có thể query
spark.sql(f"""
    CREATE DATABASE IF NOT EXISTS bronze
""")

# Tạo bảng nếu chưa tồn tại (trỏ LOCATION đến thư mục Delta)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS bronze.delta_quickstart
    USING DELTA
    LOCATION '{path}'
""")

print("Done. Wrote and registered Delta table at:", path)
spark.stop()
