# E-Commerce Real-Time Data Engineering Pipeline

An end-to-end e-commerce data engineering project that ingests events through Kafka, processes them with PySpark, stores Bronze data in Amazon S3, transforms data in Databricks into Silver/Gold analytics, and exposes analytics through FastAPI and a dashboard.

## Architecture

```text
E-commerce Events / Users
          |
          v
       Kafka
          |
          v
       PySpark
          |
          v
    Amazon S3 Bronze
          |
          v
      Databricks
      /       \
  Silver      Gold
                |
        +-------+-------+
        |               |
      FastAPI       Dashboard
        |
     Analytics
```

## Project Structure

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

## 1. Prerequisites

Install these before cloning/running the project.

| Component | Recommended version |
|---|---|
| Git | Current stable version |
| Python | 3.13 |
| Java | 21 LTS |
| Apache Kafka | 4.3.1 |
| PySpark | 4.2.0 |
| AWS CLI | v2 |
| Databricks | Workspace/cluster access for Silver/Gold |
| Power BI | Optional, for visualization |

The project is designed to run locally without Docker.

Kafka requires Java 17 or newer. PySpark 4.2.0 supports Java 17/21/25 and Python 3.10+. The recommended combination for this project is Python 3.13 + Java 21.

---

## 2. Clone the Repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd e-commerce
```

Example:

```bash
git clone https://github.com/<USERNAME>/<REPOSITORY>.git
cd e-commerce
```

---

## 3. Create the Python Virtual Environment

### Windows PowerShell

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks script execution:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

Then activate again:

```powershell
.\.venv\Scripts\Activate.ps1
```

Verify:

```powershell
python --version
where.exe python
```

`where.exe python` should point to:

```text
...\e-commerce\.venv\Scripts\python.exe
```

### Linux / macOS

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

## 4. Install Python Dependencies

With `.venv` activated:

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

The Kafka client used by this project is `confluent-kafka`.

For Python 3.13, use a current `confluent-kafka` release that provides a CPython 3.13 wheel. Do not use the old `confluent-kafka==2.5.3` build from the original development environment.

---

## 5. Configure Java

Check Java:

```bash
java -version
```

You should see Java 21 or another supported Java version.

### Windows

Set `JAVA_HOME` to your Java installation. Example:

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-21.0.12.1"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
```

Verify:

```powershell
java -version
echo $env:JAVA_HOME
```

For a permanent setting, configure `JAVA_HOME` in Windows Environment Variables.

### Linux / macOS

Set `JAVA_HOME` to your JDK installation if it is not already configured.

Linux example:

```bash
export JAVA_HOME=/path/to/jdk-21
export PATH="$JAVA_HOME/bin:$PATH"
```

macOS example:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
export PATH="$JAVA_HOME/bin:$PATH"
```

---

## 6. Windows Spark Support

This project can run PySpark on Windows without Docker.

If Spark reports a `winutils.exe`, `HADOOP_HOME`, or Hadoop native filesystem error, configure a compatible Windows Hadoop helper directory.

Example layout:

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

The Spark application can also explicitly use:

```text
spark.local.dir = C:/hadoop/tmp
```

Linux and macOS do not require Windows `winutils.exe`.

---

## 7. Install and Configure Kafka

Kafka is a separate system component. It is not installed inside the Python virtual environment.

Kafka 4.3.1 runs in KRaft mode, so ZooKeeper is not required for this setup.

Download Kafka 4.3.1 from Apache Kafka and extract it somewhere convenient.

Example Windows location:

```text
C:\kafka\kafka_2.13-4.3.1
```

Example Linux/macOS location:

```text
~/kafka/kafka_2.13-4.3.1
```

### First-time Kafka initialization

Only run the storage-format step for a new Kafka data directory.

#### Windows PowerShell

```powershell
cd "C:\kafka\kafka_2.13-4.3.1"

$CLUSTER_ID = & .\bin\windows\kafka-storage.bat random-uuid

.\bin\windows\kafka-storage.bat format --standalone -t $CLUSTER_ID -c .\config\server.properties
```

Start Kafka:

```powershell
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

Keep this terminal running.

#### Linux / macOS

```bash
cd ~/kafka/kafka_2.13-4.3.1

KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

bin/kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c config/server.properties

bin/kafka-server-start.sh config/server.properties
```

Keep this terminal running.

---

## 8. Create the Kafka Topic

Open a second terminal.

From the Kafka installation directory:

### Windows

```powershell
.\bin\windows\kafka-topics.bat --create `
  --topic ecommerce-events `
  --bootstrap-server localhost:9092 `
  --partitions 1 `
  --replication-factor 1
```

Describe the topic:

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

If the topic already exists, do not recreate it.

---

## 9. Configure Environment Variables

Copy `.env.example` to `.env`.

### Windows PowerShell

```powershell
Copy-Item .env.example .env
```

### Linux / macOS

```bash
cp .env.example .env
```

Example `.env`:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=ecommerce-events

AWS_REGION=eu-north-1
S3_BUCKET=<YOUR_UNIQUE_S3_BUCKET_NAME>
S3_BRONZE_PREFIX=bronze/ecommerce-events
S3_CHECKPOINT_PREFIX=checkpoints/ecommerce-events

SPARK_CHECKPOINT_DIR=spark_checkpoint
```

Do not commit `.env`.

AWS access keys should not be hard-coded into the repository. Prefer AWS CLI credentials or another standard AWS credential provider.

---

## 10. Configure AWS

Install AWS CLI v2 and verify:

```bash
aws --version
```

Configure the AWS CLI:

```bash
aws configure
```

Then verify the current identity:

```bash
aws sts get-caller-identity
```

The example development region used by this project is:

```text
eu-north-1
```

The AWS account used by another developer can use a different region; keep `AWS_REGION` and the S3 bucket region consistent.

---

## 11. Create the S3 Bucket

The bucket name must be globally unique.

For example:

```bash
aws s3api create-bucket   --bucket <YOUR_UNIQUE_S3_BUCKET_NAME>   --region eu-north-1   --create-bucket-configuration LocationConstraint=eu-north-1
```

Verify:

```bash
aws s3 ls
```

Test access:

```bash
aws s3 ls s3://<YOUR_UNIQUE_S3_BUCKET_NAME>/
```

Update `.env`:

```env
S3_BUCKET=<YOUR_UNIQUE_S3_BUCKET_NAME>
```

---

## 12. Verify Project Imports

From the project root with `.venv` activated:

```bash
python -c "from spark.utils.logging_config import get_logger; print('Logging import OK')"
```

```bash
python -c "import pyspark; print('PySpark:', pyspark.__version__)"
```

```bash
python -c "import confluent_kafka; print('confluent-kafka import OK')"
```

```bash
python -c "import boto3; print('boto3 import OK')"
```

---

## 13. Generate E-Commerce Events

From the project root:

```bash
python -m producer.event_generator --events 1000
```

This generates:

```text
data/events.jsonl
```

For a larger test:

```bash
python -m producer.event_generator --events 10000
```

---

## 14. Produce Events to Kafka

Make sure Kafka is running and the `ecommerce-events` topic exists.

Run:

```bash
python -m producer.kafka_producer
```

The producer publishes events to:

```text
ecommerce-events
```

Kafka broker:

```text
localhost:9092
```

---

## 15. Test Kafka -> Python Consumer

Open another terminal, activate `.venv`, and run:

```bash
python -m consumer.kafka_consumer
```

The consumer uses the `confluent-kafka` client.

You should see information similar to:

```text
Partition: 0 | Offset: 123 | Event Type: purchase | User: user_123
```

Stop with:

```text
Ctrl+C
```

---

## 16. Test Kafka -> PySpark

Run:

```bash
python -m spark.kafka_stream
```

This validates the Kafka-to-Spark streaming path.

The application dynamically resolves the Spark Kafka connector:

```text
org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0
```

---

## 17. Run Kafka -> Spark -> S3 Bronze

Before starting Bronze, verify:

```text
Kafka is running
Kafka topic exists
AWS credentials are valid
S3_BUCKET is configured
JAVA_HOME is correct
HADOOP_HOME is configured on Windows when required
```

Run:

```bash
python -u -m spark.bronze_stream
```

Bronze output:

```text
s3a://<YOUR_BUCKET>/bronze/ecommerce-events
```

Checkpoint:

```text
s3a://<YOUR_BUCKET>/checkpoints/ecommerce-events
```

### Windows first-run troubleshooting

If the S3A checkpoint fails because of a malformed local temporary path, use a local Spark checkpoint for the initial connectivity test:

```powershell
New-Item -ItemType Directory -Path "C:\hadoop\spark-checkpoints\ecommerce-bronze" -Force
```

Then configure the Bronze stream checkpoint to:

```text
C:/hadoop/spark-checkpoints/ecommerce-bronze
```

After the S3 write path is verified, restore the intended checkpoint configuration.

---

## 18. Verify Bronze Data in S3

After the Bronze stream starts receiving events:

```bash
aws s3 ls s3://<YOUR_BUCKET>/bronze/ecommerce-events/ --recursive
```

The stream partitions data by:

```text
event_date
```

Expected structure:

```text
bronze/ecommerce-events/
└── event_date=YYYY-MM-DD/
    └── part-....
```

Check checkpoints:

```bash
aws s3 ls s3://<YOUR_BUCKET>/checkpoints/ecommerce-events/ --recursive
```

---

## 19. Databricks Bronze / Silver / Gold

The Databricks portion runs in a Databricks workspace rather than the local Python environment.

Import or upload:

```text
databricks/
```

into the workspace and configure the cluster/workspace to access the S3 data.

Processing layers:

```text
S3 Bronze
   |
   v
Databricks Bronze Ingestion
   |
   v
Silver Transformation
   |
   +--------------------------+
   |            |             |
   v            v             v
Daily Sales  Product      Customer Activity
             Performance
   |
   v
Conversion Metrics
```

### Bronze

Raw/validated event data stored in S3.

### Silver

Cleaned, typed, validated, and transformed event data.

### Gold

Business-level analytics:

```text
daily_sales
product_performance
customer_activity
conversion_metrics
```

---

## 20. Start the FastAPI API

After the analytics source used by the API is configured:

```bash
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

OpenAPI JSON:

```text
http://127.0.0.1:8000/openapi.json
```

---

## 21. Start the Dashboard

In another terminal:

```bash
streamlit run dashboard/app.py
```

Streamlit will normally open a browser automatically.

---

## 22. Run Tests

Run the full test suite:

```bash
pytest
```

Or individual tests:

```bash
pytest tests/test_events.py
pytest tests/test_validation.py
pytest tests/test_transformations.py
```

---

## 23. Recommended Terminal Layout

For the complete local pipeline, use separate terminals.

### Terminal 1 — Kafka

```text
Kafka server
```

### Terminal 2 — Event Producer

```powershell
cd <project-root>
.\.venv\Scripts\Activate.ps1
python -m producer.kafka_producer
```

### Terminal 3 — Bronze Streaming

```powershell
cd <project-root>
.\.venv\Scripts\Activate.ps1
python -u -m spark.bronze_stream
```

### Terminal 4 — Debug Consumer

```powershell
cd <project-root>
.\.venv\Scripts\Activate.ps1
python -m consumer.kafka_consumer
```

### Terminal 5 — API

```powershell
cd <project-root>
.\.venv\Scripts\Activate.ps1
uvicorn api.main:app --reload
```

### Terminal 6 — Dashboard

```powershell
cd <project-root>
.\.venv\Scripts\Activate.ps1
streamlit run dashboard/app.py
```

Databricks jobs run separately against the S3 Bronze/Silver layers.

---

## 24. Common Problems

### `ModuleNotFoundError: No module named 'config'`

Run commands from the project root:

```bash
cd e-commerce
```

Use module execution:

```bash
python -m producer.kafka_producer
python -m consumer.kafka_consumer
python -m spark.kafka_stream
python -m spark.bronze_stream
```

Avoid running package files from inside their subdirectories.

### `JAVA_HOME is not set`

Check:

```bash
java -version
```

Then set `JAVA_HOME` to the JDK installation.

### Spark reports `winutils.exe` or Hadoop errors

Windows:

```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:HADOOP_HOME_DIR = "C:\hadoop"
$env:PATH = "C:\hadoop\bin;$env:PATH"
```

Verify:

```powershell
Test-Path "C:\hadoop\bin\winutils.exe"
```

### S3A credential error

Run:

```bash
aws sts get-caller-identity
```

Then:

```bash
aws s3 ls s3://<YOUR_BUCKET>/
```

Do not place AWS secrets directly in source code.

### S3A local temporary-directory error on Windows

Create:

```powershell
New-Item -ItemType Directory -Path "C:\hadoop\tmp" -Force
```

Set:

```powershell
$env:LOCAL_DIRS = "C:\hadoop\tmp"
```

and explicitly configure Spark/S3A local buffering to use:

```text
C:/hadoop/tmp
```

### Kafka connection refused

Verify the Kafka server is running and the broker is:

```text
localhost:9092
```

Also verify:

```text
ecommerce-events
```

exists.

### `confluent-kafka` tries to compile from source

Upgrade packaging tools:

```bash
python -m pip install --upgrade pip setuptools wheel
```

Then install a current `confluent-kafka` release that has a wheel for your Python/OS combination.

### Spark cannot resolve the Kafka connector

The streaming jobs use:

```text
org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0
```

The first run may require internet access so Spark can download Maven dependencies.

---

## 25. Recreate Kafka Data from Scratch

Only do this when you intentionally want a completely clean local Kafka environment.

Stop Kafka first.

Remove the configured Kafka log directory from the Kafka `server.properties` location, then initialize Kafka storage again:

```text
kafka-storage random-uuid
kafka-storage format --standalone ...
```

Recreate:

```text
ecommerce-events
```

Do not delete Kafka storage on a production cluster.

---

## 26. Git and Secrets

The repository should contain:

```text
.env.example
```

but not:

```text
.env
```

Recommended `.gitignore` entries:

```text
.venv/
__pycache__/
*.pyc
.env
.pytest_cache/
spark_checkpoint/
checkpoints/
```

Never commit:

```text
AWS access keys
AWS secret keys
Databricks personal access tokens
private API tokens
```

---

## 27. End-to-End Run Order

For a clean machine:

```text
1. Install Git
2. Install Python 3.13
3. Install Java 21
4. Install Apache Kafka 4.3.1
5. Configure Kafka in KRaft mode
6. Create ecommerce-events topic
7. Clone this repository
8. Create and activate .venv
9. Install requirements.txt
10. Configure .env
11. Install/configure AWS CLI
12. Configure AWS credentials
13. Create or select the S3 bucket
14. Verify AWS access
15. Generate events
16. Start Kafka
17. Start kafka_producer
18. Start Spark Bronze stream
19. Verify Bronze data in S3
20. Run Databricks Bronze/Silver/Gold jobs
21. Start FastAPI
22. Start Streamlit dashboard
23. Connect Power BI to the Gold analytics layer
```

---

## 28. Useful Commands Cheat Sheet

Activate environment:

```powershell
.\.venv\Scripts\Activate.ps1
```

```bash
source .venv/bin/activate
```

Generate events:

```bash
python -m producer.event_generator --events 1000
```

Produce to Kafka:

```bash
python -m producer.kafka_producer
```

Consume Kafka:

```bash
python -m consumer.kafka_consumer
```

Kafka -> Spark:

```bash
python -m spark.kafka_stream
```

Kafka -> Spark -> S3 Bronze:

```bash
python -u -m spark.bronze_stream
```

Run tests:

```bash
pytest
```

Run API:

```bash
uvicorn api.main:app --reload
```

Run dashboard:

```bash
streamlit run dashboard/app.py
```

Verify AWS identity:

```bash
aws sts get-caller-identity
```

Verify S3:

```bash
aws s3 ls s3://<YOUR_BUCKET>/
```

---

## 29. Notes for Contributors

Always run the project from the repository root.

Prefer:

```bash
python -m <package>.<module>
```

instead of:

```bash
python <package>/<module>.py
```

This keeps imports such as:

```python
from config.config import config
from spark.utils.logging_config import get_logger
```

consistent.

Keep environment-specific values in `.env`, not in Python source files.

---

## 30. Data Flow Summary

```text
JSON event generation
        |
        v
Kafka event streaming
        |
        v
PySpark Structured Streaming
        |
        v
S3 Bronze Parquet
        |
        v
Databricks transformations
        |
        v
Silver cleaned data
        |
        v
Gold business metrics
        |
        +------> FastAPI
        |
        +------> Dashboard
        |
        +------> Power BI
```

This separation keeps ingestion, storage, transformation, analytics, and presentation concerns independent.

---

## Official References

Apache Kafka:
https://kafka.apache.org/

Apache Spark:
https://spark.apache.org/

PySpark 4.2.0:
https://spark.apache.org/docs/4.2.0/

AWS CLI:
https://aws.amazon.com/cli/

Amazon S3:
https://aws.amazon.com/s3/

Confluent Kafka Python client:
https://pypi.org/project/confluent-kafka/

Databricks:
https://www.databricks.com/

FastAPI:
https://fastapi.tiangolo.com/

Streamlit:
https://streamlit.io/
