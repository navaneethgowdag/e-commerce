from pathlib import Path

import pandas as pd
from databricks import sql

from config.config import config
from spark.utils.logging_config import get_logger


logger = get_logger("bronze_ingestion")


# ============================================================
# Configuration
# ============================================================

LOCAL_BRONZE_PATH = Path(config.BRONZE_OUTPUT_PATH)

CATALOG = config.DATABRICKS_CATALOG
SCHEMA = config.DATABRICKS_SCHEMA
TABLE = config.DATABRICKS_BRONZE_TABLE

TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE}"

# Number of rows sent to Databricks in one INSERT batch.
BATCH_SIZE = 1000


# ============================================================
# Databricks SQL connection
# ============================================================

def create_sql_connection():
    if not config.DATABRICKS_SERVER_HOSTNAME:
        raise ValueError(
            "DATABRICKS_SERVER_HOSTNAME is not configured."
        )

    if not config.DATABRICKS_HTTP_PATH:
        raise ValueError(
            "DATABRICKS_HTTP_PATH is not configured."
        )

    if not config.DATABRICKS_TOKEN:
        raise ValueError(
            "DATABRICKS_TOKEN is not configured."
        )

    logger.info("Connecting to Databricks SQL Warehouse...")

    connection = sql.connect(
        server_hostname=config.DATABRICKS_SERVER_HOSTNAME,
        http_path=config.DATABRICKS_HTTP_PATH,
        access_token=config.DATABRICKS_TOKEN,
    )

    logger.info("Connected to Databricks SQL Warehouse.")

    return connection


# ============================================================
# Find local Parquet files
# ============================================================

def find_parquet_files():
    if not LOCAL_BRONZE_PATH.exists():
        raise FileNotFoundError(
            f"Bronze directory not found: {LOCAL_BRONZE_PATH}"
        )

    parquet_files = [
        file
        for file in LOCAL_BRONZE_PATH.rglob("*.parquet")
        if "_temporary" not in file.parts
    ]

    parquet_files.sort()

    return parquet_files


# ============================================================
# Create Bronze table
# ============================================================

def create_bronze_table(connection):

    logger.info(
        f"Creating/verifying table: {TABLE_NAME}"
    )

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        kafka_key STRING,
        topic STRING,
        `partition` INT,
        `offset` BIGINT,
        kafka_timestamp TIMESTAMP,

        event_id STRING,
        user_id STRING,
        session_id STRING,
        event_type STRING,
        `timestamp` STRING,
        event_date STRING,

        device STRING,
        traffic_source STRING,
        customer_name STRING,
        segment STRING,
        country STRING,
        market STRING,
        region STRING,

        product_id STRING,
        category STRING,
        sub_category STRING,
        product_name STRING,

        product_price DOUBLE,
        quantity INT,
        discount_percent DOUBLE,
        sales DOUBLE,

        ship_mode STRING,
        order_priority STRING,

        removed_quantity INT,
        search_query STRING,

        event_timestamp TIMESTAMP,
        event_date_parsed DATE
    )
    USING DELTA
    """

    with connection.cursor() as cursor:
        cursor.execute(create_sql)

    logger.info(
        f"Bronze table ready: {TABLE_NAME}"
    )


# ============================================================
# Clear existing Bronze table
# ============================================================

def truncate_bronze_table(connection):

    logger.info(
        f"Clearing existing Bronze data: {TABLE_NAME}"
    )

    with connection.cursor() as cursor:
        cursor.execute(
            f"TRUNCATE TABLE {TABLE_NAME}"
        )

    logger.info("Existing Bronze data cleared.")


# ============================================================
# Convert pandas values to database-safe values
# ============================================================

def clean_value(value):

    if pd.isna(value):
        return None

    # Convert pandas Timestamp to Python datetime
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    # Convert pandas integer/float/string types
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass

    return value


# ============================================================
# Convert DataFrame rows to tuples
# ============================================================

def dataframe_to_rows(df):

    expected_columns = [
        "kafka_key",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",

        "event_id",
        "user_id",
        "session_id",
        "event_type",
        "timestamp",
        "event_date",

        "device",
        "traffic_source",
        "customer_name",
        "segment",
        "country",
        "market",
        "region",

        "product_id",
        "category",
        "sub_category",
        "product_name",

        "product_price",
        "quantity",
        "discount_percent",
        "sales",

        "ship_mode",
        "order_priority",

        "removed_quantity",
        "search_query",

        "event_timestamp",
        "event_date_parsed",
    ]

    # Make sure every expected column exists.
    for column in expected_columns:
        if column not in df.columns:
            df[column] = None

    df = df[expected_columns]

    rows = []

    for row in df.itertuples(index=False, name=None):
        cleaned_row = tuple(
            clean_value(value)
            for value in row
        )

        rows.append(cleaned_row)

    return rows


# ============================================================
# Insert rows into Bronze table
# ============================================================

def insert_rows(connection, rows):

    if not rows:
        return 0

    insert_sql = f"""
    INSERT INTO {TABLE_NAME} (
        kafka_key,
        topic,
        `partition`,
        `offset`,
        kafka_timestamp,

        event_id,
        user_id,
        session_id,
        event_type,
        `timestamp`,
        event_date,

        device,
        traffic_source,
        customer_name,
        segment,
        country,
        market,
        region,

        product_id,
        category,
        sub_category,
        product_name,

        product_price,
        quantity,
        discount_percent,
        sales,

        ship_mode,
        order_priority,

        removed_quantity,
        search_query,

        event_timestamp,
        event_date_parsed
    )
    VALUES (
        ?,
        ?,
        ?,
        ?,
        ?,

        ?,
        ?,
        ?,
        ?,
        ?,
        ?,

        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,

        ?,
        ?,
        ?,
        ?,

        ?,
        ?,
        ?,
        ?,

        ?,
        ?,

        ?,
        ?,

        ?,
        ?
    )
    """

    total_inserted = 0

    with connection.cursor() as cursor:

        for start in range(0, len(rows), BATCH_SIZE):

            batch = rows[
                start:start + BATCH_SIZE
            ]

            cursor.executemany(
                insert_sql,
                batch
            )

            total_inserted += len(batch)

            logger.info(
                f"Inserted {total_inserted} rows..."
            )

    return total_inserted


# ============================================================
# Process one Parquet file
# ============================================================

def process_parquet_file(
    connection,
    parquet_file,
):

    logger.info(
        f"Reading: {parquet_file}"
    )

    try:

        df = pd.read_parquet(
            parquet_file
        )

        rows = dataframe_to_rows(df)

        inserted = insert_rows(
            connection,
            rows
        )

        logger.info(
            f"Loaded {inserted} rows from "
            f"{parquet_file.name}"
        )

        return inserted

    except Exception as error:

        logger.error(
            f"Failed processing {parquet_file}: "
            f"{error}"
        )

        raise


# ============================================================
# Verify Bronze table
# ============================================================

def verify_bronze_table(connection):

    logger.info(
        "=========================================="
    )

    logger.info(
        "VERIFYING BRONZE TABLE"
    )

    logger.info(
        "=========================================="
    )

    with connection.cursor() as cursor:

        # Row count
        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM {TABLE_NAME}
            """
        )

        result = cursor.fetchone()

        row_count = result[0]

        logger.info(
            f"Bronze row count: {row_count}"
        )

        # Event types
        cursor.execute(
            f"""
            SELECT
                event_type,
                COUNT(*) AS event_count
            FROM {TABLE_NAME}
            GROUP BY event_type
            ORDER BY event_count DESC
            """
        )

        event_types = cursor.fetchall()

        logger.info(
            "Event type counts:"
        )

        for row in event_types:
            logger.info(
                f"  {row[0]} -> {row[1]}"
            )

        # Sample records
        cursor.execute(
            f"""
            SELECT *
            FROM {TABLE_NAME}
            LIMIT 5
            """
        )

        sample_rows = cursor.fetchall()

        logger.info(
            "Bronze sample:"
        )

        for row in sample_rows:
            logger.info(row)

    return row_count


# ============================================================
# Main
# ============================================================

def main():

    logger.info(
        "=========================================="
    )

    logger.info(
        "LOCAL BRONZE -> DATABRICKS BRONZE"
    )

    logger.info(
        "=========================================="
    )

    logger.info(
        f"Local Bronze: {LOCAL_BRONZE_PATH}"
    )

    logger.info(
        f"Databricks Table: {TABLE_NAME}"
    )

    # --------------------------------------------------------
    # 1. Find Parquet files
    # --------------------------------------------------------

    parquet_files = find_parquet_files()

    logger.info(
        f"Found {len(parquet_files)} Parquet files."
    )

    if not parquet_files:
        raise RuntimeError(
            "No Parquet files found."
        )

    # --------------------------------------------------------
    # 2. Connect to Databricks
    # --------------------------------------------------------

    connection = create_sql_connection()

    try:

        # ----------------------------------------------------
        # 3. Create Bronze Delta table
        # ----------------------------------------------------

        create_bronze_table(
            connection
        )

        # ----------------------------------------------------
        # 4. Full refresh
        # ----------------------------------------------------
        #
        # This makes the script deterministic:
        #
        # Local Parquet
        #       ↓
        # bronze_events
        #
        # Running the script again will not duplicate data.
        #
        # ----------------------------------------------------

        truncate_bronze_table(
            connection
        )

        # ----------------------------------------------------
        # 5. Load every Parquet file
        # ----------------------------------------------------

        total_rows = 0

        for index, parquet_file in enumerate(
            parquet_files,
            start=1
        ):

            logger.info(
                f"[{index}/{len(parquet_files)}] "
                f"Processing Parquet file"
            )

            rows_inserted = process_parquet_file(
                connection,
                parquet_file
            )

            total_rows += rows_inserted

        # ----------------------------------------------------
        # 6. Verify
        # ----------------------------------------------------

        logger.info(
            f"Total rows sent to Databricks: "
            f"{total_rows}"
        )

        actual_row_count = verify_bronze_table(
            connection
        )

        if actual_row_count != total_rows:

            raise RuntimeError(
                "Row count mismatch. "
                f"Python inserted: {total_rows}, "
                f"Databricks contains: {actual_row_count}"
            )

        logger.info(
            "=========================================="
        )

        logger.info(
            "BRONZE INGESTION COMPLETED SUCCESSFULLY"
        )

        logger.info(
            "=========================================="
        )

    finally:

        connection.close()

        logger.info(
            "Databricks connection closed."
        )


if __name__ == "__main__":
    main()