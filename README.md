# E-Commerce Data Engineering Pipeline

An end-to-end e-commerce data engineering project that processes customer and transaction events using **Apache Kafka, PySpark Structured Streaming, Databricks, SQL, and Power BI**.

The pipeline demonstrates real-time event ingestion, local Bronze data storage, cloud-based analytical processing, and business-ready Gold datasets.

---

## Architecture

```text
                E-Commerce Events
                       │
                       ▼
              Event Generator
                       │
                       ▼
                    Kafka
                       │
                       ▼
          PySpark Structured Streaming
                       │
                       ▼
              Local Bronze Parquet
                       │
                       ▼
          Databricks Bronze Delta
                       │
                       ▼
             Databricks Silver
                       │
                       ▼
              Databricks Gold
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
      Daily Sales   Product      Customer
                    Performance   Activity
                       │
                       ▼
              Conversion Metrics
                       │
                       ▼
                    Power BI

Note: AWS S3 is not used in the current version of this project. Bronze Parquet files are stored locally before being loaded into Databricks.

Technologies
Python
Apache Kafka
PySpark Structured Streaming
Databricks SQL Warehouse
Databricks Delta Tables
SQL
Parquet
Power BI
Git & GitHub
Project Structure
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
│   ├── test_spark_s3.py
│   ├── test_hadoop_aws.py
│   │
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
│   │
│   ├── silver/
│   │   └── silver_transformation.py
│   │
│   └── gold/
│       └── gold_transformation.py
│
├── dashboard/
│
├── tests/
│
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
Pipeline
1. E-Commerce Event Generation

The event generator creates simulated e-commerce events containing information about:

Customers
Users
Sessions
Products
Categories
Devices
Traffic sources
Locations
Quantities
Prices
Discounts
Sales
Shipping
Order priority

Example event:

{
  "event_id": "53667b35-da43-448a-a7f0-f47208d451bf",
  "user_id": "user_062215",
  "session_id": "801553ae-b477-4c2f-a59e-30ab4a92ffe5",
  "event_type": "remove_from_cart",
  "timestamp": "2024-02-14T15:50:34",
  "event_date": "2024-02-14",
  "device": "tablet",
  "traffic_source": "social",
  "customer_name": "Sophia Johnson",
  "segment": "Home Office",
  "country": "Canada",
  "market": "Canada",
  "region": "East",
  "product_id": "FUR-CHA-002",
  "category": "Furniture",
  "sub_category": "Chairs",
  "product_name": "Executive Chair",
  "product_price": 389.57,
  "quantity": 2,
  "discount_percent": 25,
  "sales": 584.36,
  "ship_mode": "Same Day",
  "order_priority": "Low",
  "removed_quantity": 2
}
2. Kafka

Apache Kafka acts as the real-time event streaming platform.

The producer publishes events to:

ecommerce-events

Kafka allows the application to continuously send and process events.

For this local project, Kafka runs in KRaft mode, so ZooKeeper is not required.

3. PySpark Structured Streaming

PySpark consumes events from Kafka and processes them in micro-batches.

The Bronze streaming job:

Connects to Kafka
Reads JSON events
Applies the event schema
Validates event data
Adds Kafka metadata
Converts timestamps
Processes events in micro-batches
Writes Parquet files locally

The current streaming configuration processes Kafka data using:

maxOffsetsPerTrigger = 10,000

For example, 100,000 Kafka events can be processed approximately as:

Batch 0 → 10,000
Batch 1 → 10,000
Batch 2 → 10,000
...
Batch 9 → 10,000

The streaming job should be allowed to process the required Kafka records before stopping it.

4. Local Bronze Layer

The Bronze layer stores the processed Kafka events as Parquet files on the local machine.

Example:

C:\hadoop\bronze-staging\
│
├── event_date_parsed=2016-07-19\
│   ├── part-00000-....parquet
│   └── ...
│
├── event_date_parsed=2016-07-20\
│   └── ...
│
└── ...

The files are partitioned by:

event_date_parsed

Parquet is used because it provides efficient columnar storage for analytical processing.

5. Databricks Bronze

After PySpark finishes processing the required Kafka events, the local Parquet files are loaded into Databricks using:

databricks/bronze/bronze_ingestion.py

The process is:

Local Parquet
      │
      ▼
bronze_ingestion.py
      │
      ▼
Databricks SQL Warehouse
      │
      ▼
bronze_events

The Bronze table is:

ecommerce_catalog.ecommerce.bronze_events

The ingestion script:

Finds all local Parquet files.
Connects to Databricks SQL Warehouse.
Creates or verifies the Bronze Delta table.
Clears previous Bronze data.
Loads all Parquet records.
Verifies the final row count.
6. Silver Layer

The Silver transformation reads:

ecommerce_catalog.ecommerce.bronze_events

and creates:

ecommerce_catalog.ecommerce.silver_events

The Silver layer performs data cleaning and standardization.

Transformations
Trim whitespace
Standardize categorical fields
Convert timestamps
Validate dates
Clean numeric values
Handle negative quantities
Validate discount percentages
Remove records missing critical fields
Remove duplicate event_id values
Calculate net_quantity
Calculate discount_amount

Example:

Bronze

quantity = 2
removed_quantity = 1

        ↓

Silver

net_quantity = 1

Another example:

product_price = 389.57
quantity = 2
discount_percent = 25

        ↓

discount_amount = 194.79
7. Gold Layer

The Gold layer converts the cleaned Silver data into business-ready analytical datasets.

Four Gold tables are created.

Daily Sales
gold_daily_sales

Contains metrics such as:

Daily sales
Total events
Unique customers
Unique sessions
Unique products
Quantity
Removed quantity
Net quantity
Discount amount
Average sales
Average discount
Product Performance
gold_product_performance

Contains:

Product
Product category
Sub-category
Total events
Unique customers
Unique sessions
Quantity
Net quantity
Sales
Discount amount
Average product price
Average discount
Customer Activity
gold_customer_activity

Contains:

Customer
Segment
Country
Market
Region
Sessions
Products
Quantity
Net quantity
Sales
Discount amount
First activity
Last activity
Conversion Metrics
gold_conversion_metrics

Provides a session-level conversion funnel:

Sessions
   ↓
Views
   ↓
Add to Cart
   ↓
Remove from Cart
   ↓
Purchase

Metrics include:

Total sessions
View sessions
Add-to-cart sessions
Remove-from-cart sessions
Purchase sessions
View rate
Add-to-cart rate
Cart-to-purchase rate
Overall conversion rate
8. Power BI

Power BI connects to the Databricks SQL Warehouse and uses the Gold tables for visualization.

Possible dashboard sections include:

Executive Dashboard
Total Sales
Total Customers
Total Sessions
Total Quantity
Conversion Rate
Daily Sales Trend
Product Analytics
Sales by Product
Sales by Category
Sales by Sub-category
Product Quantity
Product Performance
Customer Analytics
Customer Sales
Customer Activity
Customer Segments
Geographic Analysis
Conversion Analytics
Session Funnel
View Rate
Add-to-Cart Rate
Purchase Rate
Overall Conversion Rate
Data Architecture

The project follows the Medallion Architecture:

                    RAW EVENTS
                        │
                        ▼
                ┌──────────────┐
                │    BRONZE    │
                │              │
                │ Raw Kafka    │
                │ Events       │
                └──────┬───────┘
                       │
                       ▼
                ┌──────────────┐
                │    SILVER    │
                │              │
                │ Cleaned &    │
                │ Standardized │
                │ Data         │
                └──────┬───────┘
                       │
                       ▼
                ┌──────────────┐
                │     GOLD     │
                │              │
                │ Business     │
                │ Aggregations │
                └──────┬───────┘
                       │
                       ▼
                    POWER BI
Databricks Tables
ecommerce_catalog
│
└── ecommerce
    │
    ├── bronze_events
    │
    ├── silver_events
    │
    ├── gold_daily_sales
    │
    ├── gold_product_performance
    │
    ├── gold_customer_activity
    │
    └── gold_conversion_metrics
Setup
1. Clone the Repository
git clone https://github.com/navaneethgowdag/e-commerce.git
cd e-commerce
2. Create Virtual Environment
python -m venv .venv

Activate:

.venv\Scripts\Activate.ps1
3. Install Dependencies
pip install -r requirements.txt
4. Configure Environment Variables

Create .env using .env.example.

Example:

KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=ecommerce-events

DATABRICKS_SERVER_HOSTNAME=your-databricks-host
DATABRICKS_HTTP_PATH=your-http-path
DATABRICKS_TOKEN=your-token

DATABRICKS_CATALOG=ecommerce_catalog
DATABRICKS_SCHEMA=ecommerce

DATABRICKS_BRONZE_TABLE=bronze_events
DATABRICKS_SILVER_TABLE=silver_events

BRONZE_OUTPUT_PATH=C:\hadoop\bronze-staging

Do not commit .env or credentials to GitHub.

Running the Pipeline
Step 1 — Start Kafka

Start the local Kafka broker in KRaft mode.

Verify:

Test-NetConnection localhost -Port 9092
Step 2 — Create Kafka Topic
cd C:\kafka_2.13-4.3.1

.\bin\windows\kafka-topics.bat `
  --bootstrap-server localhost:9092 `
  --create `
  --topic ecommerce-events `
  --partitions 1 `
  --replication-factor 1

Verify:

.\bin\windows\kafka-topics.bat `
  --bootstrap-server localhost:9092 `
  --describe `
  --topic ecommerce-events
Step 3 — Start Event Producer

From the project directory:

python -u -m producer.kafka_producer

The producer publishes e-commerce events to Kafka.

Step 4 — Start Bronze Streaming
python -u -m spark.bronze_stream

The streaming job reads Kafka and writes local Parquet files.

Do not stop the process simply because 5–6 minutes have passed.

Allow it to process the required Kafka records.

For example, if Kafka contains 100,000 events:

100,000 Kafka events
        ↓
PySpark Structured Streaming
        ↓
100,000 local Bronze records

After the required records have been processed and written, stop the streaming job with:

Ctrl + C
Step 5 — Load Bronze into Databricks
python -u -m databricks.bronze.bronze_ingestion

This loads all local Bronze Parquet files into:

ecommerce_catalog.ecommerce.bronze_events

Verify the Bronze row count in Databricks:

SELECT COUNT(*) AS total_rows
FROM ecommerce_catalog.ecommerce.bronze_events;
Step 6 — Run Silver Transformation
python -u -m databricks.silver.silver_transformation

This creates:

ecommerce_catalog.ecommerce.silver_events

Verify:

SELECT COUNT(*) AS total_rows
FROM ecommerce_catalog.ecommerce.silver_events;
Step 7 — Run Gold Transformation
python -u -m databricks.gold.gold_transformation

This creates:

gold_daily_sales
gold_product_performance
gold_customer_activity
gold_conversion_metrics
Verification Queries
Bronze
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT event_id) AS unique_events
FROM ecommerce_catalog.ecommerce.bronze_events;
Silver
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT event_id) AS unique_events
FROM ecommerce_catalog.ecommerce.silver_events;
Gold Daily Sales
SELECT *
FROM ecommerce_catalog.ecommerce.gold_daily_sales
ORDER BY event_date;
Gold Product Performance
SELECT *
FROM ecommerce_catalog.ecommerce.gold_product_performance
ORDER BY total_sales DESC;
Gold Customer Activity
SELECT *
FROM ecommerce_catalog.ecommerce.gold_customer_activity
ORDER BY total_sales DESC;
Gold Conversion Metrics
SELECT *
FROM ecommerce_catalog.ecommerce.gold_conversion_metrics;
Key Data Engineering Concepts

This project demonstrates:

Real-time data ingestion
Event-driven architecture
Apache Kafka
Kafka producers and consumers
PySpark Structured Streaming
Micro-batch processing
JSON event processing
Schema enforcement
Data validation
Parquet storage
Databricks SQL Warehouse
Delta Lake
Medallion Architecture
Data quality transformations
Deduplication
SQL analytics
Business aggregations
Power BI reporting
Pipeline verification
Environment configuration
Git version control
Project Flow
1. Generate e-commerce events
              ↓
2. Publish events to Kafka
              ↓
3. Consume events with PySpark
              ↓
4. Write Bronze Parquet locally
              ↓
5. Load Parquet into Databricks Bronze
              ↓
6. Clean and transform Bronze → Silver
              ↓
7. Aggregate Silver → Gold
              ↓
8. Connect Gold to Power BI
              ↓
9. Build business dashboards



