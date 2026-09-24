import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # -------------------------
    # Kafka
    # -------------------------
    KAFKA_BOOTSTRAP_SERVERS = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092"
    )

    KAFKA_TOPIC = os.getenv(
        "KAFKA_TOPIC",
        "ecommerce-events"
    )

    # -------------------------
    # Local Bronze
    # -------------------------
    EVENTS_FILE = os.getenv(
        "EVENTS_FILE",
        "data/events.jsonl"
    )

    BRONZE_OUTPUT_PATH = os.getenv(
        "BRONZE_OUTPUT_PATH",
        r"C:\hadoop\bronze-staging"
    )

    SPARK_CHECKPOINT_DIR = os.getenv(
        "SPARK_CHECKPOINT_DIR",
        r"C:\hadoop\spark-checkpoints\ecommerce-bronze"
    )

    # -------------------------
    # Databricks
    # -------------------------
    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
    DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

    DATABRICKS_CATALOG = os.getenv(
        "DATABRICKS_CATALOG",
        "ecommerce_catalog"
    )

    DATABRICKS_SCHEMA = os.getenv(
        "DATABRICKS_SCHEMA",
        "ecommerce"
    )

    DATABRICKS_BRONZE_TABLE = os.getenv(
        "DATABRICKS_BRONZE_TABLE",
        "bronze_events"
    )

    DATABRICKS_SILVER_TABLE = os.getenv(
        "DATABRICKS_SILVER_TABLE",
        "silver_events"
    )

    DATABRICKS_BRONZE_VOLUME = os.getenv(
        "DATABRICKS_BRONZE_VOLUME",
        "/Volumes/ecommerce_catalog/ecommerce/bronze_volume"
    )


config = Config()