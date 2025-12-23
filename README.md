# 🚀 Open Lakehouse integrated with Data Quality Mangagement Framework
### A Fully Local, Open-Source Lakehouse for Ingestion, Storage, Processing, and Orchestration

This project implements a complete **Open Lakehouse architecture** using 100% open-source technologies. It is designed for end-to-end data ingestion, processing, storage, orchestration, and analytics - all running locally via Docker.

The architecture follows the **Medallion Model (Bronze → Silver → Gold)** and provides a unified environment for ETL/ELT, metadata management, SQL analytics, and data quality pipelines.

### Data Quality Mangagement Framework
The Data Quality Management Framework is tightly integrated into the Lakehouse to ensure trusted, reliable data.
#### Introduction to Project: 
[Nghiên cứu và xây dựng Lakehouse kết hợp Framework Quản lý Chất lượng Dữ liệu](https://youtu.be/I_r0h9v-vzA)
#### Project Demo: 
[DEMO-Nghiên cứu và xây dựng Lakehouse kết hợp Framework Quản lý Chất lượng Dữ liệu](https://youtu.be/FE0A83WuH-k)



---
## 🧱 Lakehouse Architecture Summary

### 🗄️ **Data Lake Storage**
- **MinIO** - S3-compatible object storage for all Bronze, Silver, and Gold layers.

### 📁 **Supported File Formats**
- **Parquet**, **ORC**, **CSV**, **JSON**, and other semi-structured formats.

### 🧩 **Open Table Format**
- **Delta Lake** - ACID transactions, schema evolution, time travel, and optimized Parquet storage.

### 🗃️ **Metastore**
- **Hive Metastore** - central metadata catalog for Delta Lake and Trino.

### 💻**Processing Engine**
- **Apache Spark + Delta Lake** - Performs ETL/ELT jobs and medallion transformations.

### ⚙️ **Orchestration Layer**
- **Apache Airflow 3.1**  - For automate the process: Schedules ingestion, processing, and enrichment jobs.

### 📊 **Compute Engine**
- **Trino** - distributed SQL query engine for analytics across the entire Lakehouse.

### 🐳 **Containerization & Deployment**
- **Docker & Docker Compose** - orchestrates MinIO, Hive Metastore, Trino, Spark, and Airflow into a fully local Lakehouse environment.

  
## 🛡️ Data Quality Management Framework

The Data Quality Management Framework is fully integrated into the Lakehouse to ensure **reliable, trustworthy data**.

### 🔍 Automated Data Quality Checks
- Completeness - Data is considered complete when all the data required for a particular use is present and available to be used. 
- Validity - Validity is defined as the extent to which the data conforms to the expected format, type, and range.

Prevents data quality issues from propagating to downstream layers.

---

### 🚨 Alerting & Monitoring
- Automatically detects data quality violations
- Sends alerts when rules are breached through email
- Highlights abnormal records for investigation

