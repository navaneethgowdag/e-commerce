# E-Commerce Real-Time Data Engineering Pipeline

An end-to-end data engineering project that demonstrates a practical e-commerce analytics pipeline using **Apache Kafka, PySpark, Amazon S3, Databricks, and Power BI**.

The project ingests e-commerce events, streams them through Kafka, processes them with PySpark Structured Streaming, stores raw Bronze data in Amazon S3, transforms the data through Databricks Bronze/Silver/Gold layers, and exposes the Gold analytics through Power BI.

## GitHub Repository

https://github.com/navaneethgowdag/e-commerce.git

---

# Architecture

```text
                  E-COMMERCE EVENTS
                         |
                         v
                    Event Generator
                         |
                         v
                       Kafka
                         |
                         v
                PySpark Structured
                    Streaming
                         |
                         v
                  AWS S3 - Bronze
                         |
                         v
                 Databricks Bronze
                         |
                         v
                 Databricks Silver
                         |
                         v
                  Databricks Gold
                         |
             +-----------+-----------+
             |           |           |
             v           v           v
        Daily Sales   Product    Customer/
        Analytics     Analytics   Conversion
             \           |           /
              \          |          /
               +---------+---------+
                         |
                         v
                     Power BI
```

## Technology Stack

| Component | Technology |
|---|---|
| Event generation | Python + Faker |
| Message streaming | Apache Kafka (KRaft) |
| Stream processing | PySpark Structured Streaming |
| Object storage | Amazon S3 |
| Data lake format | Parquet |
| Analytics platform | Databricks |
| Bronze/Silver/Gold | Databricks + Delta |
| Databricks local automation | Databricks SQL Connector for Python |
| BI / Visualization | Power BI |
| API | FastAPI |
| Optional dashboard | Streamlit |
| Testing | Pytest |
| Containerization | Not required |

This project is designed to run locally without Docker.

---

# Project Structure

```text
e-commerce/
│
├── data/
│   └── events.jsonl
│
├── producer/
│   ├── event_generator.py
│   └── kafka_producer.py
│
├── consumer/
│   └── kafka_consumer.py
│
├── spark/
│   ├── kafka_stream.py
│   ├── bronze_stream.py
│   ├── silver_transform.py
│   └── utils/
│       ├── schema.py
│       ├── validation.py
│       └── logging_config.py
│
├── config/
│   ├── config.py
│   └── aws_config.py
│
├── databricks/
│   ├── bronze/
│   │   └── bronze_ingestion.py
│   ├── silver/
│   │   └── silver_transformation.py
│   └── gold/
│       ├── daily_sales.py
│       ├── product_performance.py
│       ├── customer_activity.py
│       └── conversion_metrics.py
│
├── api/
│   ├── main.py
│   ├── routes/
│   │   ├── sales.py
│   │   ├── products.py
│   │   └── analytics.py
│   ├── services/
│   │   └── analytics_service.py
│   └── schemas/
│       └── response_models.py
│
├── dashboard/
│   └── app.py
│
├── tests/
│   ├── test_events.py
│   ├── test_validation.py
│   └── test_transformations.py
│
├── .env
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

---

# 1. Prerequisites

Install the following:

| Component | Project version / recommendation |
|---|---|
| Git | Current stable version |
| Python | 3.13 |
| Java | 21 LTS |
| Apache Kafka | 4.3.1 |
| PySpark | 4.2.0 |
| AWS CLI | v2 |
| Databricks | Workspace + SQL Warehouse |
| Power BI Desktop | Required for BI reporting |
| Docker | Not required |

The development environment used for this project is:

```text
Python 3.13
Java 21
Kafka 4.3.1
PySpark 4.2.0
AWS S3
Databricks
Power BI
```

---

# 2. Clone the Repository

```bash
git clone https://github.com/navaneethgowdag/e-commerce.git
cd e-commerce
```

Always run Python commands from the project root:

```text
e-commerce/
```

---

# 3. Create the Python Virtual Environment

## Windows PowerShell

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks activation:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

Then:

```powershell
.\.venv\Scripts\Activate.ps1
```

Verify:

```powershell
python --version
where.exe python
```

The Python path should point to:

```text
e-commerce\.venv\Scripts\python.exe
```

## Linux / macOS

```bash
python3.13 -m venv .venv
source .venv/bin/activate
```

Verify:

```bash
python --version
which python
```

---

# 4. Install Python Dependencies

Activate `.venv`, then:

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

If the project dependencies are not yet in `requirements.txt`, install the packages used by the project:

```bash
pip install pyspark faker python-dotenv boto3 databricks-sql-connector confluent-kafka fastapi uvicorn streamlit pandas pytest
```

The project uses `confluent-kafka` for the Python Kafka consumer/producer components.

For Python 3.13, use a `confluent-kafka` release that provides a compatible wheel for your operating system and Python version.

---

# 5. Configure Java

Check:

```bash
java -version
```

The project uses Java 21.

## Windows

Example:

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-21.0.12.1"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
```

Verify:

```powershell
java -version
echo $env:JAVA_HOME
```

For a permanent configuration, add `JAVA_HOME` to Windows Environment Variables.

## Linux

```bash
export JAVA_HOME=/path/to/jdk-21
export PATH="$JAVA_HOME/bin:$PATH"
```

## macOS

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
export PATH="$JAVA_HOME/bin:$PATH"
```

---

# 6. Windows PySpark Support

The local PySpark application runs on Windows.

If Spark/Hadoop reports Windows filesystem errors, configure the Windows Hadoop helper.

Expected layout:

```text
C:\hadoop\
└── bin\
    └── winutils.exe
```

Set:

```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:HADOOP_HOME_DIR = "C:\hadoop"
$env:PATH = "C:\hadoop\bin;$env:PATH"
```

Create a temporary directory:

```powershell
New-Item -ItemType Directory -Path "C:\hadoop\tmp" -Force
$env:LOCAL_DIRS = "C:\hadoop\tmp"
```

The Bronze streaming application can explicitly use:

```text
C:/hadoop/tmp
```

for Spark local files.

Linux and macOS do not require `winutils.exe`.

---

# 7. Configure Kafka

Kafka is a separate system component and is not installed inside `.venv`.

This project uses Kafka in **KRaft mode**, so ZooKeeper is not required.

Download Apache Kafka 4.3.1 and extract it.

Example Windows path:

```text
C:\kafka\kafka_2.13-4.3.1
```

## First-time KRaft initialization

Only do this for a new Kafka storage directory.

### Windows PowerShell

```powershell
cd "C:\kafka\kafka_2.13-4.3.1"

$CLUSTER_ID = & .\bin\windows\kafka-storage.bat random-uuid

.\bin\windows\kafka-storage.bat format --standalone -t $CLUSTER_ID -c .\config\server.properties
```

Start Kafka:

```powershell
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

Keep Kafka running.

---

# 8. Create the Kafka Topic

Open another terminal.

### Windows

```powershell
cd "C:\kafka\kafka_2.13-4.3.1"

.\bin\windows\kafka-topics.bat --create `
  --topic ecommerce-events `
  --bootstrap-server localhost:9092 `
  --partitions 1 `
  --replication-factor 1
```

Verify:

```powershell
.\bin\windows\kafka-topics.bat --describe `
  --topic ecommerce-events `
  --bootstrap-server localhost:9092
```

### Linux / macOS

```bash
bin/kafka-topics.sh --create \
  --topic ecommerce-events \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

If `ecommerce-events` already exists, keep it.

---

# 9. Configure `.env`

Copy the example file.

## Windows

```powershell
Copy-Item .env.example .env
```

## Linux / macOS

```bash
cp .env.example .env
```

Example configuration:

```env
# ============================================================
# Kafka
# ============================================================

KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=ecommerce-events


# ============================================================
# AWS
# ============================================================

AWS_REGION=eu-north-1
S3_BUCKET=<YOUR_S3_BUCKET_NAME>

S3_BRONZE_PREFIX=bronze/ecommerce-events
S3_CHECKPOINT_PREFIX=checkpoints/ecommerce-events

SPARK_CHECKPOINT_DIR=spark_checkpoint


# ============================================================
# Event generator
# ============================================================

EVENTS_FILE=data/events.jsonl


# ============================================================
# Databricks
# ============================================================

DATABRICKS_HOST=https://<YOUR_WORKSPACE_HOST>
DATABRICKS_TOKEN=<YOUR_DATABRICKS_TOKEN>
DATABRICKS_SERVER_HOSTNAME=<YOUR_SERVER_HOSTNAME>
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<YOUR_WAREHOUSE_ID>

DATABRICKS_CATALOG=ecommerce_catalog
DATABRICKS_SCHEMA=ecommerce

DATABRICKS_BRONZE_TABLE=bronze_events
DATABRICKS_SILVER_TABLE=silver_events
```

Never commit `.env`.

Never put real AWS credentials or Databricks tokens in source code.

---

# 10. Configure AWS

Install AWS CLI v2.

Verify:

```bash
aws --version
```

Configure credentials:

```bash
aws configure
```

Verify authentication:

```bash
aws sts get-caller-identity
```

Verify S3:

```bash
aws s3 ls
```

For the example environment, the AWS region is:

```text
eu-north-1
```

The S3 bucket name must be globally unique.

---

# 11. Create / Configure the S3 Bucket

Example:

```bash
aws s3api create-bucket \
  --bucket <YOUR_S3_BUCKET_NAME> \
  --region eu-north-1 \
  --create-bucket-configuration LocationConstraint=eu-north-1
```

Verify:

```bash
aws s3 ls s3://<YOUR_S3_BUCKET_NAME>/
```

Update `.env`:

```env
S3_BUCKET=<YOUR_S3_BUCKET_NAME>
```

The application writes Bronze data under:

```text
s3://<YOUR_S3_BUCKET_NAME>/bronze/ecommerce-events/
```

and checkpoint data under:

```text
s3://<YOUR_S3_BUCKET_NAME>/checkpoints/ecommerce-events/
```

---

# 12. Configure Databricks

Create a Databricks workspace and make sure a SQL Warehouse is available.

The project uses Databricks through its SQL interface from the local Python environment.

## Databricks Workspace Host

Example:

```env
DATABRICKS_HOST=https://dbc-b34bcd2a-605e.cloud.databricks.com
```

Use your own workspace host if different.

## SQL Warehouse HTTP Path

In Databricks:

```text
SQL
→ SQL Warehouses
→ your warehouse
→ Connection Details
```

Copy:

```text
Server hostname
HTTP path
```

Example HTTP path:

```text
/sql/1.0/warehouses/xxxxxxxxxxxxxxxx
```

Put it into `.env`:

```env
DATABRICKS_SERVER_HOSTNAME=dbc-b34bcd2a-605e.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxxxxxxxxxxxxxx
```

## Databricks Token

For local automation, create a Databricks Personal Access Token if PAT authentication is enabled in your workspace.

Keep the token only in `.env`:

```env
DATABRICKS_TOKEN=<YOUR_TOKEN>
```

Do not commit the token.

---

# 13. Configure the S3 External Location in Databricks

The Databricks workspace must have access to your S3 Bronze location.

The Bronze S3 path used by this project is:

```text
s3://<YOUR_S3_BUCKET_NAME>/bronze/ecommerce-events
```

Configure a Unity Catalog External Location for the Bronze path.

Conceptually:

```text
AWS S3
   ↓
Unity Catalog External Location
   ↓
Databricks
```

You can then use Databricks SQL / Unity Catalog to work with the Bronze data.

---

# 14. Verify Python Imports

From the project root:

```powershell
python -c "import pyspark; print('PySpark:', pyspark.__version__)"
```

```powershell
python -c "import confluent_kafka; print('Kafka client OK')"
```

```powershell
python -c "import boto3; print('boto3 OK')"
```

```powershell
python -c "import databricks; print('Databricks package OK')"
```

---

# 15. Generate the E-Commerce Dataset

The event generator creates JSONL data.

The generated timestamps are distributed across:

```text
2020-01-01
through
2026-12-31
```

The generated event model currently contains fields such as:

```text
event_id
user_id
product_id
event_type
timestamp
price
quantity
device
country
search_term
```

Countries and event types are randomized to create a more varied dataset.

Generate 1,000 events:

```powershell
python -m producer.event_generator --events 1000
```

Generate 10,000:

```powershell
python -m producer.event_generator --events 10000
```

Generate 100,000:

```powershell
python -m producer.event_generator --events 100000
```

The dataset is written to:

```text
data/events.jsonl
```

---

# 16. Verify Generated Dates

Windows PowerShell:

```powershell
Get-Content data\events.jsonl -First 10
```

The timestamps should span different days across the configured 2020–2026 range.

---

# 17. Start the Kafka Producer

Make sure Kafka is running.

From the project root:

```powershell
python -m producer.kafka_producer
```

The producer reads:

```text
data/events.jsonl
```

and sends events to:

```text
ecommerce-events
```

on:

```text
localhost:9092
```

---

# 18. Test the Kafka Consumer

In another terminal:

```powershell
python -m consumer.kafka_consumer
```

The consumer uses the `confluent-kafka` Python client.

Example output:

```text
Partition: 0 | Offset: 123 | Event Type: purchase | User: usr_1234
```

Stop it with:

```text
Ctrl+C
```

---

# 19. Test Kafka -> PySpark

Run:

```powershell
python -m spark.kafka_stream
```

This validates:

```text
Kafka
  ↓
PySpark Structured Streaming
  ↓
structured events
```

The application uses the Spark Kafka connector:

```text
org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0
```

---

# 20. Run Kafka -> PySpark -> S3 Bronze

Configure the Windows Spark temporary directories first:

```powershell
New-Item -ItemType Directory -Path "C:\hadoop\tmp" -Force

$env:HADOOP_HOME = "C:\hadoop"
$env:HADOOP_HOME_DIR = "C:\hadoop"
$env:LOCAL_DIRS = "C:\hadoop\tmp"
$env:PATH = "C:\hadoop\bin;$env:PATH"
```

Run:

```powershell
python -u -m spark.bronze_stream
```

The stream keeps running.

New Kafka events are processed automatically and written to S3 every trigger interval configured in the application.

You do not rerun the Bronze application for every event.

---

# 21. Verify Bronze Data in S3

```powershell
aws s3 ls s3://<YOUR_S3_BUCKET_NAME>/bronze/ecommerce-events/ --recursive
```

Expected structure is similar to:

```text
bronze/ecommerce-events/
├── _spark_metadata/
└── event_date=YYYY-MM-DD/
    ├── part-....parquet
    └── ...
```

The Bronze data is stored as Parquet and partitioned by `event_date`.

---

# 22. Databricks Bronze Ingestion

The Databricks ingestion scripts are intended to run through the **Databricks SQL Warehouse**, not as local PySpark programs that directly read `s3://`.

The local script:

```text
databricks/bronze/bronze_ingestion.py
```

uses:

```text
DATABRICKS_HOST
DATABRICKS_SERVER_HOSTNAME
DATABRICKS_HTTP_PATH
DATABRICKS_TOKEN
```

and the Databricks SQL Connector.

Run:

```powershell
python -m databricks.bronze.bronze_ingestion
```

The intended Bronze table is:

```text
ecommerce_catalog.ecommerce.bronze_events
```

The table points to the S3 Bronze data through the Databricks/Unity Catalog configuration.

---

# 23. Verify Databricks Bronze

In Databricks SQL:

```sql
SELECT COUNT(*) AS total_records
FROM ecommerce_catalog.ecommerce.bronze_events;
```

Inspect records:

```sql
SELECT *
FROM ecommerce_catalog.ecommerce.bronze_events
LIMIT 20;
```

Inspect schema:

```sql
DESCRIBE TABLE ecommerce_catalog.ecommerce.bronze_events;
```

---

# 24. Run Silver Transformation

The Silver layer cleans and standardizes the Bronze data.

Run:

```powershell
python -m databricks.silver.silver_transformation
```

The target table is:

```text
ecommerce_catalog.ecommerce.silver_events
```

The Silver transformation currently performs operations such as:

```text
trim identifiers
lowercase event_type
lowercase device
uppercase country
validate price
validate quantity
remove invalid event types
deduplicate event_id
derive event_year
derive event_month
derive event_day
calculate purchase_amount
calculate item_value
```

Verify:

```sql
SELECT COUNT(*) AS total_records
FROM ecommerce_catalog.ecommerce.silver_events;
```

---

# 25. Run Gold Analytics

Run these after Silver succeeds.

## Daily Sales

```powershell
python -m databricks.gold.daily_sales
```

Creates:

```text
ecommerce_catalog.ecommerce.gold_daily_sales
```

## Product Performance

```powershell
python -m databricks.gold.product_performance
```

Creates:

```text
ecommerce_catalog.ecommerce.gold_product_performance
```

## Customer Activity

```powershell
python -m databricks.gold.customer_activity
```

Creates:

```text
ecommerce_catalog.ecommerce.gold_customer_activity
```

## Conversion Metrics

```powershell
python -m databricks.gold.conversion_metrics
```

Creates:

```text
ecommerce_catalog.ecommerce.gold_conversion_metrics
```

---

# 26. Verify All Databricks Tables

Run:

```sql
SHOW TABLES IN ecommerce_catalog.ecommerce;
```

Expected project tables:

```text
bronze_events
silver_events
gold_daily_sales
gold_product_performance
gold_customer_activity
gold_conversion_metrics
```

---

# 27. Connect Power BI

Power BI is the primary BI/presentation layer for this project.

Use the Databricks SQL Warehouse.

In Power BI Desktop:

```text
Home
→ Get Data
→ More...
→ Databricks
→ Connect
```

Enter:

```text
Server Hostname:
dbc-b34bcd2a-605e.cloud.databricks.com
```

Use your actual hostname if different.

Then:

```text
HTTP Path:
/sql/1.0/warehouses/xxxxxxxxxxxxxxxx
```

Use the value from:

```text
Databricks
→ SQL
→ SQL Warehouses
→ your warehouse
→ Connection Details
```

Authenticate using the Databricks authentication method available in your workspace.

For the current PAT-based setup, use your Databricks token.

---

# 28. Load Only Gold Tables into Power BI

Use:

```text
ecommerce_catalog
    ↓
ecommerce
    ├── gold_daily_sales
    ├── gold_product_performance
    ├── gold_customer_activity
    └── gold_conversion_metrics
```

Do not load Bronze or Silver into the reporting model unless you have a specific analytical reason.

The recommended reporting architecture is:

```text
Bronze → Silver → Gold → Power BI
```

---

# 29. Suggested Power BI Report Pages

## Page 1 — Executive Overview

Use:

```text
Total Sales
Total Orders
Units Sold
Unique Customers
Average Order Value
```

Charts:

```text
Daily Sales Trend
Daily Orders
Daily Units Sold
```

## Page 2 — Product Analytics

Use:

```text
Product ID
Revenue
Units Sold
Product Views
Add to Cart
Purchases
Average Selling Price
```

## Page 3 — Customer Analytics

Use:

```text
User ID
Active Days
Orders
Units Purchased
Total Spend
Average Order Value
```

## Page 4 — Conversion Analytics

Use:

```text
Page Views
Product Views
Add to Cart
Purchases

Product View Rate
Add to Cart Rate
Purchase Rate
Overall Conversion Rate
```

---

# 30. Optional FastAPI API

The project also includes a FastAPI layer.

Run:

```powershell
uvicorn api.main:app --reload
```

API:

```text
http://127.0.0.1:8000
```

Swagger UI:

```text
http://127.0.0.1:8000/docs
```

OpenAPI:

```text
http://127.0.0.1:8000/openapi.json
```

---

# 31. Optional Streamlit Dashboard

Streamlit is an optional presentation layer.

Run:

```powershell
streamlit run dashboard/app.py
```

It can read the same Databricks Gold tables used by Power BI.

---

# 32. Run Tests

Full test suite:

```bash
pytest
```

Individual tests:

```bash
pytest tests/test_events.py
pytest tests/test_validation.py
pytest tests/test_transformations.py
```

---

# 33. Recommended Terminal Setup

## Terminal 1 — Kafka

```text
Kafka server
```

## Terminal 2 — Producer

```powershell
cd <project-root>
.\.venv\Scripts\Activate.ps1
python -m producer.kafka_producer
```

## Terminal 3 — Bronze Stream

```powershell
cd <project-root>
.\.venv\Scripts\Activate.ps1
python -u -m spark.bronze_stream
```

## Terminal 4 — Consumer

```powershell
cd <project-root>
.\.venv\Scripts\Activate.ps1
python -m consumer.kafka_consumer
```

## Terminal 5 — FastAPI (optional)

```powershell
cd <project-root>
.\.venv\Scripts\Activate.ps1
uvicorn api.main:app --reload
```

## Terminal 6 — Streamlit (optional)

```powershell
cd <project-root>
.\.venv\Scripts\Activate.ps1
streamlit run dashboard/app.py
```

Databricks Bronze/Silver/Gold commands use the Databricks SQL Warehouse connection configured in `.env`.

---

# 34. Important: Python Module Execution

Run package modules from the project root.

Prefer:

```bash
python -m producer.kafka_producer
python -m consumer.kafka_consumer
python -m spark.kafka_stream
python -m spark.bronze_stream
python -m databricks.bronze.bronze_ingestion
python -m databricks.silver.silver_transformation
python -m databricks.gold.daily_sales
```

instead of:

```bash
python producer/kafka_producer.py
python spark/bronze_stream.py
```

This keeps project imports consistent.

---

# 35. Important: Databricks Scripts Are Not the Same as Local Spark Scripts

Local streaming:

```text
spark/bronze_stream.py
```

does:

```text
Kafka
  ↓
local PySpark
  ↓
S3 Bronze
```

Databricks scripts:

```text
databricks/bronze/bronze_ingestion.py
databricks/silver/silver_transformation.py
databricks/gold/*.py
```

use:

```text
Local Python
  ↓
Databricks SQL Connector
  ↓
Databricks SQL Warehouse
  ↓
Unity Catalog / S3
```

Do not treat a local PySpark process and a Databricks cluster as the same Spark environment.

---

# 36. Clear Project Data Without Deleting the Infrastructure

If you need a fresh dataset while keeping the project configuration:

## Clear S3 objects

```powershell
aws s3 rm s3://<YOUR_S3_BUCKET_NAME>/ --recursive
```

Be careful: this removes all objects in the bucket.

## Clear Databricks table rows

For Delta Silver/Gold tables:

```sql
TRUNCATE TABLE ecommerce_catalog.ecommerce.silver_events;

TRUNCATE TABLE ecommerce_catalog.ecommerce.gold_daily_sales;

TRUNCATE TABLE ecommerce_catalog.ecommerce.gold_product_performance;

TRUNCATE TABLE ecommerce_catalog.ecommerce.gold_customer_activity;

TRUNCATE TABLE ecommerce_catalog.ecommerce.gold_conversion_metrics;
```

Keep the table definitions.

For the external Bronze Parquet table, clear the underlying S3 objects rather than assuming `TRUNCATE TABLE` is appropriate for an external Parquet location.

---

# 37. New Datasets With Different Column Names

The pipeline architecture does not require future datasets to use the same source column names.

For a different dataset:

```text
New source columns
        ↓
source schema
        ↓
Bronze
        ↓
Silver column mapping / standardization
        ↓
Gold analytics
        ↓
Power BI
```

For example:

```text
Source:
CustomerID
TransactionDate
Amount

Silver:
customer_id
transaction_date
amount
```

When changing the dataset, update the following based on the real source schema:

```text
spark/utils/schema.py
producer/event_generator.py       (when generating events)
databricks/bronze/bronze_ingestion.py
databricks/silver/silver_transformation.py
databricks/gold/*.py
Power BI model
```

Do not force a new dataset into the previous e-commerce schema without inspecting the source first.

---

# 38. Common Problems

## ModuleNotFoundError

Run from:

```text
e-commerce/
```

and use:

```bash
python -m package.module
```

## Java / JAVA_HOME error

```powershell
java -version
echo $env:JAVA_HOME
```

## Windows Hadoop helper error

```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:HADOOP_HOME_DIR = "C:\hadoop"
$env:PATH = "C:\hadoop\bin;$env:PATH"
```

## Kafka connection refused

Check:

```text
localhost:9092
```

and verify Kafka is running.

## S3 access problem

Run:

```bash
aws sts get-caller-identity
```

then:

```bash
aws s3 ls s3://<YOUR_S3_BUCKET_NAME>/
```

## Databricks connection problem

Verify all three values:

```text
DATABRICKS_SERVER_HOSTNAME
DATABRICKS_HTTP_PATH
DATABRICKS_TOKEN
```

The HTTP path must belong to the SQL Warehouse you are connecting to.

## Local PySpark says `No FileSystem for scheme "s3"`

This usually means the local Spark process is being asked to read `s3://` without the required S3A/Hadoop configuration.

For Databricks processing, use the Databricks SQL Connector approach implemented in the `databricks/` scripts instead of trying to make local Windows PySpark act like Databricks.

## Power BI still shows old data

If using Import mode, Power BI stores an imported copy of the data.

Use:

```text
Home
→ Refresh
```

to retrieve the current source data.

DirectQuery can query Databricks instead of importing the full source dataset, subject to Power BI/Databricks performance and modeling considerations.

---

# 39. Git and Secrets

Do not commit:

```text
.env
AWS access keys
AWS secret keys
Databricks tokens
Databricks personal access tokens
API tokens
```

Recommended `.gitignore`:

```text
.venv/
__pycache__/
*.pyc
.env
.pytest_cache/
spark_checkpoint/
checkpoints/
*.log
```

The repository should contain:

```text
.env.example
```

but not:

```text
.env
```

---

# 40. Complete End-to-End Run Order

For a fresh machine:

```text
1. Install Git
2. Install Python 3.13
3. Install Java 21
4. Install Kafka 4.3.1
5. Configure Kafka KRaft
6. Create ecommerce-events topic
7. Clone GitHub repository
8. Create .venv
9. Activate .venv
10. Install requirements
11. Configure Java
12. Configure Windows Hadoop helper if required
13. Install AWS CLI
14. Configure AWS credentials
15. Create/select S3 bucket
16. Configure .env
17. Create Databricks workspace
18. Create/start Databricks SQL Warehouse
19. Configure Databricks S3 External Location
20. Configure Databricks host/token/HTTP path
21. Generate events
22. Start Kafka
23. Start Kafka producer
24. Start Bronze Spark stream
25. Verify Parquet files in S3
26. Run Databricks Bronze ingestion
27. Verify bronze_events
28. Run Silver transformation
29. Verify silver_events
30. Run Gold daily sales
31. Run Gold product performance
32. Run Gold customer activity
33. Run Gold conversion metrics
34. Verify all Gold tables
35. Connect Power BI to Databricks
36. Load Gold tables
37. Build Power BI report
38. Optionally run FastAPI
39. Optionally run Streamlit
```

---

# 41. Final Data Flow

```text
                    EVENT GENERATION
                           |
                           v
                         KAFKA
                           |
                           v
                 PYSPARK STREAMING
                           |
                           v
                    AWS S3 BRONZE
                           |
                           v
              DATABRICKS BRONZE TABLE
                           |
                           v
              DATABRICKS SILVER TABLE
                           |
                           v
                     GOLD TABLES
                           |
          +----------------+----------------+
          |                |                |
          v                v                v
     Daily Sales      Product Metrics   Customer /
                                      Conversion
          \                |                /
           \               |               /
            +--------------+--------------+
                           |
                           v
                       POWER BI
```

The project demonstrates a complete data engineering workflow from event generation and Kafka ingestion through cloud storage, transformation, analytics, and BI reporting.
