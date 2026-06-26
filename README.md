# 🚀 Near-Realtime Data Streaming Architecture

This project demonstrates a near-realtime data streaming architecture for tracking comission, leveraging Change Data Capture (CDC). Data modifications from the source database are captured automatically, streamed through a message broker, processed via a streaming engine, stored in a Data Lakehouse layer, and synchronized into an OLAP database for analytics while triggering alert services.

## 🛠️ Tech Stack
* **Database & CDC:** PostgreSQL, Debezium
* **Source:** PostgreSQL (kèm Debezium CDC)
* **Message Broker:** Apache Kafka, Zookeeper
* **Processing:** Apache Spark (Structured Streaming)
* **Storage:** MinIO (Object Storage)
* **Warehouse:** ClickHouse
* **Monitoring:** Prometheus, Grafana

## 🌊 Data Flow
Application → Postgres → Debezium → Kafka → Spark
*→ MinIO (Data Lake)
*→ Email Service (Alerts)
*→ ClickHouse (Analytics)

## ⚙️ Preparation
### 1. Folder Directory Setup
Run the following command in your terminal at the project root directory to initialize data volumes for Docker. This ensures data persistence when containers are stopped or removed:
```
mkdir -p data/clickhouse \
         data/email \
         data/kafka \
         data/minio \
         data/postgres \
         data/zookeeper/data \
         data/zookeeper/log
```
### 2. Environment Variables Configuration (.env)
Create a file named .env in the root directory of your project and populate it with the following configurations (replace the values indicated by <-- with your actual information):


--- PostgreSQL Configuration ---

POSTGRES_DB_NAME=your_database_name <-- Điền tên database nguồn
POSTGRES_USERNAME=your_user_name     <-- Điền tài khoản kết nối DB
POSTGRES_PASSWORD=your_password     <-- Điền mật khẩu kết nối DB

--- Kafka & Debezium Configuration (Optional for easier management) ---

KAFKA_BOOTSTRAP_SERVER=localhost:9092
DEBEZIUM_API_URL=http://localhost:8083/connectors

--- MinIO Configuration ---

MINIO_ROOT_USER=your_minio_user         <-- Tài khoản quản trị MinIO
MINIO_ROOT_PASSWORD=your_minio_password <-- Mật khẩu quản trị MinIO

--- ClickHouse Configuration ---

CLICKHOUSE_USER=your_clickhouse_user         <-- Tài khoản truy cập ClickHouse
CLICKHOUSE_PASSWORD=your_clickhouse_password <-- Mật khẩu truy cập ClickHouse
CLICKHOUSE_DB=your_analytics_db_name         <-- Tên database dùng phân tích

## 🚀 Setup & Deployment (Step-by-Step)
### Step 1: Setup MinIO (Object Storage)

#### 1.Start the MinIO service independently:

`docker-compose up -d --build minio`

#### 2.Open your browser and navigate to the MinIO Console at: http://localhost:9001

#### 3.Log in using the credentials (default if not modified in .env):
* **Username:** admin
* **Password:** admin

#### 4.Create a new bucket and name it exactly: lakehouse

### Step 2: Setup PostgreSQL & Migration
#### 1.Start the source database service:

`docker-compose up -d --build postgres`

#### 2.Run the migration script to automatically initialize database schemas and seed initial mock data:

`python migrate.py`

### Step 3: Run the Entire System
Once the storage layer (MinIO) and source database (PostgreSQL) are up and configured, start all remaining system services (Kafka, Debezium, Spark, ClickHouse, Prometheus, Grafana...):

`docker-compose up -d --build`

## 📊 Monitoring & Observability (Observer)
Once all containers are successfully running (Up), you can monitor the data pipeline and system health through the following user interfaces:
|Service	|Functional Description	|Access URL (Localhost)|
| :--- | :--- | :--- |
|PySpark UI	|Track execution, health, and performance of streaming jobs	|http://localhost:4040/|
|Kafka UI	|Monitor topics, inspect real-time messages, and manage consumers	|http://localhost:8080/|
|MinIO Console	|Inspect raw object storage data structures (Parquet files)	|http://localhost:9001/|
|Grafana	|Centralized dashboard for infrastructure and system metrics	|http://localhost:3000/|
|Prometheus Check	|Inspect connection endpoints health status	|http://localhost:9090/targets|

**⚠️ Critical Notice**

Before running docker-compose up, kindly verify and ensure that the required ports (4040, 8080, 9000, 9001, 3000, 9090, 5432, 8123) are completely available (free) and not occupied or blocked by any other background processes or local services running on your machine.