from databricks import sql

from config.config import config
from spark.utils.logging_config import get_logger


# ============================================================
# Configuration
# ============================================================

logger = get_logger("silver_transformation")

CATALOG = config.DATABRICKS_CATALOG
SCHEMA = config.DATABRICKS_SCHEMA

BRONZE_TABLE = config.DATABRICKS_BRONZE_TABLE
SILVER_TABLE = config.DATABRICKS_SILVER_TABLE

BRONZE_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{BRONZE_TABLE}"
SILVER_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{SILVER_TABLE}"


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

    logger.info(
        "Connecting to Databricks SQL Warehouse..."
    )

    connection = sql.connect(
        server_hostname=config.DATABRICKS_SERVER_HOSTNAME,
        http_path=config.DATABRICKS_HTTP_PATH,
        access_token=config.DATABRICKS_TOKEN,
    )

    logger.info(
        "Connected to Databricks SQL Warehouse."
    )

    return connection


# ============================================================
# Check Bronze table
# ============================================================

def verify_bronze_table(connection):

    logger.info(
        f"Checking Bronze table: {BRONZE_TABLE_NAME}"
    )

    with connection.cursor() as cursor:

        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM {BRONZE_TABLE_NAME}
            """
        )

        row_count = cursor.fetchone()[0]

    logger.info(
        f"Bronze row count: {row_count}"
    )

    if row_count == 0:
        raise RuntimeError(
            "Bronze table is empty."
        )

    return row_count


# ============================================================
# Create Silver table
# ============================================================

def create_silver_table(connection):

    logger.info(
        f"Creating/replacing Silver table: "
        f"{SILVER_TABLE_NAME}"
    )

    create_sql = f"""
    CREATE OR REPLACE TABLE {SILVER_TABLE_NAME}
    USING DELTA
    AS

    WITH cleaned_data AS (

        SELECT

            -- Kafka metadata
            kafka_key,
            topic,
            `partition`,
            `offset`,
            kafka_timestamp,

            -- Event identifiers
            TRIM(event_id) AS event_id,
            TRIM(user_id) AS user_id,
            TRIM(session_id) AS session_id,

            -- Standardized event type
            LOWER(TRIM(event_type)) AS event_type,

            -- Original timestamp
            TRIM(`timestamp`) AS timestamp,

            -- Proper timestamp
            event_timestamp,

            -- Date
            COALESCE(
                event_date_parsed,
                CAST(event_date AS DATE),
                CAST(event_timestamp AS DATE)
            ) AS event_date,

            -- Customer/device information
            LOWER(TRIM(device)) AS device,
            LOWER(TRIM(traffic_source)) AS traffic_source,

            TRIM(customer_name) AS customer_name,
            TRIM(segment) AS segment,

            -- Geography
            TRIM(country) AS country,
            TRIM(market) AS market,
            TRIM(region) AS region,

            -- Product information
            TRIM(product_id) AS product_id,
            TRIM(category) AS category,
            TRIM(sub_category) AS sub_category,
            TRIM(product_name) AS product_name,

            -- Numeric fields
            ROUND(CAST(product_price AS DOUBLE), 2)
                AS product_price,

            CAST(quantity AS INT)
                AS quantity,

            ROUND(
                CAST(discount_percent AS DOUBLE),
                2
            ) AS discount_percent,

            ROUND(
                CAST(sales AS DOUBLE),
                2
            ) AS sales,

            LOWER(TRIM(ship_mode))
                AS ship_mode,

            LOWER(TRIM(order_priority))
                AS order_priority,

            CAST(removed_quantity AS INT)
                AS removed_quantity,

            TRIM(search_query)
                AS search_query

        FROM {BRONZE_TABLE_NAME}

        WHERE event_id IS NOT NULL
          AND TRIM(event_id) <> ''

          AND user_id IS NOT NULL
          AND TRIM(user_id) <> ''

          AND session_id IS NOT NULL
          AND TRIM(session_id) <> ''

          AND event_type IS NOT NULL
          AND TRIM(event_type) <> ''

          AND product_id IS NOT NULL
          AND TRIM(product_id) <> ''

          AND event_timestamp IS NOT NULL
    ),

    validated_data AS (

        SELECT

            kafka_key,
            topic,
            `partition`,
            `offset`,
            kafka_timestamp,

            event_id,
            user_id,
            session_id,
            event_type,
            timestamp,
            event_timestamp,
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

            -- Prevent negative prices
            CASE
                WHEN product_price < 0 THEN NULL
                ELSE product_price
            END AS product_price,

            -- Prevent negative quantities
            CASE
                WHEN quantity < 0 THEN 0
                ELSE quantity
            END AS quantity,

            -- Keep discount between 0 and 100
            CASE
                WHEN discount_percent < 0 THEN 0
                WHEN discount_percent > 100 THEN 100
                ELSE discount_percent
            END AS discount_percent,

            sales,

            ship_mode,
            order_priority,

            -- Prevent negative removed quantity
            CASE
                WHEN removed_quantity < 0 THEN 0
                ELSE removed_quantity
            END AS removed_quantity,

            search_query

        FROM cleaned_data
    ),

    deduplicated_data AS (

        SELECT

            *,

            ROW_NUMBER() OVER (
                PARTITION BY event_id
                ORDER BY
                    event_timestamp DESC,
                    `offset` DESC
            ) AS row_number

        FROM validated_data
    )

    SELECT

        kafka_key,
        topic,
        `partition`,
        `offset`,
        kafka_timestamp,

        event_id,
        user_id,
        session_id,
        event_type,
        timestamp,
        event_timestamp,
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
        removed_quantity,

        -- Quantity remaining after removals
        GREATEST(
            quantity - COALESCE(removed_quantity, 0),
            0
        ) AS net_quantity,

        discount_percent,

        -- Calculated discount amount
        ROUND(
            COALESCE(product_price, 0)
            * COALESCE(quantity, 0)
            * COALESCE(discount_percent, 0)
            / 100,
            2
        ) AS discount_amount,

        sales,

        ship_mode,
        order_priority,

        search_query

    FROM deduplicated_data

    WHERE row_number = 1
    """

    with connection.cursor() as cursor:
        cursor.execute(create_sql)

    logger.info(
        f"Silver table created successfully: "
        f"{SILVER_TABLE_NAME}"
    )


# ============================================================
# Verify Silver table
# ============================================================

def verify_silver_table(connection):

    logger.info(
        "=========================================="
    )

    logger.info(
        "VERIFYING SILVER TABLE"
    )

    logger.info(
        "=========================================="
    )

    with connection.cursor() as cursor:

        # ----------------------------------------------------
        # Row count
        # ----------------------------------------------------

        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM {SILVER_TABLE_NAME}
            """
        )

        row_count = cursor.fetchone()[0]

        logger.info(
            f"Silver row count: {row_count}"
        )

        # ----------------------------------------------------
        # Unique event IDs
        # ----------------------------------------------------

        cursor.execute(
            f"""
            SELECT COUNT(DISTINCT event_id)
            FROM {SILVER_TABLE_NAME}
            """
        )

        unique_events = cursor.fetchone()[0]

        logger.info(
            f"Unique event IDs: {unique_events}"
        )

        # ----------------------------------------------------
        # Event type counts
        # ----------------------------------------------------

        cursor.execute(
            f"""
            SELECT
                event_type,
                COUNT(*) AS event_count
            FROM {SILVER_TABLE_NAME}
            GROUP BY event_type
            ORDER BY event_count DESC
            """
        )

        event_types = cursor.fetchall()

        logger.info(
            "Silver event type counts:"
        )

        for row in event_types:
            logger.info(
                f"  {row[0]} -> {row[1]}"
            )

        # ----------------------------------------------------
        # Sample records
        # ----------------------------------------------------

        cursor.execute(
            f"""
            SELECT
                event_id,
                user_id,
                event_type,
                event_timestamp,
                product_id,
                product_name,
                quantity,
                removed_quantity,
                net_quantity,
                discount_percent,
                discount_amount,
                sales
            FROM {SILVER_TABLE_NAME}
            LIMIT 5
            """
        )

        sample_rows = cursor.fetchall()

        logger.info(
            "Silver sample records:"
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
        "BRONZE -> SILVER TRANSFORMATION"
    )

    logger.info(
        "=========================================="
    )

    logger.info(
        f"Bronze table: {BRONZE_TABLE_NAME}"
    )

    logger.info(
        f"Silver table: {SILVER_TABLE_NAME}"
    )

    connection = create_sql_connection()

    try:

        # ----------------------------------------------------
        # 1. Verify Bronze
        # ----------------------------------------------------

        bronze_count = verify_bronze_table(
            connection
        )

        # ----------------------------------------------------
        # 2. Create Silver
        # ----------------------------------------------------

        create_silver_table(
            connection
        )

        # ----------------------------------------------------
        # 3. Verify Silver
        # ----------------------------------------------------

        silver_count = verify_silver_table(
            connection
        )

        # ----------------------------------------------------
        # 4. Final validation
        # ----------------------------------------------------

        logger.info(
            f"Bronze rows: {bronze_count}"
        )

        logger.info(
            f"Silver rows: {silver_count}"
        )

        if silver_count > bronze_count:
            raise RuntimeError(
                "Silver row count cannot be greater "
                "than Bronze row count."
            )

        logger.info(
            "=========================================="
        )

        logger.info(
            "SILVER TRANSFORMATION COMPLETED "
            "SUCCESSFULLY"
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