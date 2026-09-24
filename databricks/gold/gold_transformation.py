from databricks import sql

from config.config import config
from spark.utils.logging_config import get_logger


# ============================================================
# Configuration
# ============================================================

logger = get_logger("gold_transformation")

CATALOG = config.DATABRICKS_CATALOG
SCHEMA = config.DATABRICKS_SCHEMA

SILVER_TABLE = config.DATABRICKS_SILVER_TABLE

SILVER_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{SILVER_TABLE}"

DAILY_SALES_TABLE = f"{CATALOG}.{SCHEMA}.gold_daily_sales"
PRODUCT_TABLE = f"{CATALOG}.{SCHEMA}.gold_product_performance"
CUSTOMER_TABLE = f"{CATALOG}.{SCHEMA}.gold_customer_activity"
CONVERSION_TABLE = f"{CATALOG}.{SCHEMA}.gold_conversion_metrics"


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
# Verify Silver table
# ============================================================

def verify_silver_table(connection):

    logger.info(
        f"Checking Silver table: {SILVER_TABLE_NAME}"
    )

    with connection.cursor() as cursor:

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

    if row_count == 0:
        raise RuntimeError(
            "Silver table is empty."
        )

    return row_count


# ============================================================
# 1. Daily Sales
# ============================================================

def create_daily_sales(connection):

    logger.info(
        f"Creating Gold table: {DAILY_SALES_TABLE}"
    )

    query = f"""
    CREATE OR REPLACE TABLE {DAILY_SALES_TABLE}
    USING DELTA
    AS

    SELECT

        event_date,

        COUNT(*) AS total_events,

        COUNT(
            DISTINCT user_id
        ) AS unique_customers,

        COUNT(
            DISTINCT session_id
        ) AS unique_sessions,

        COUNT(
            DISTINCT product_id
        ) AS unique_products,

        SUM(
            COALESCE(sales, 0)
        ) AS total_sales,

        SUM(
            COALESCE(quantity, 0)
        ) AS total_quantity,

        SUM(
            COALESCE(removed_quantity, 0)
        ) AS total_removed_quantity,

        SUM(
            COALESCE(net_quantity, 0)
        ) AS total_net_quantity,

        SUM(
            COALESCE(discount_amount, 0)
        ) AS total_discount_amount,

        AVG(
            COALESCE(sales, 0)
        ) AS average_event_sales,

        AVG(
            COALESCE(discount_percent, 0)
        ) AS average_discount_percent

    FROM {SILVER_TABLE_NAME}

    WHERE event_date IS NOT NULL

    GROUP BY event_date
    """

    with connection.cursor() as cursor:
        cursor.execute(query)

    logger.info(
        "Daily sales table created successfully."
    )


# ============================================================
# 2. Product Performance
# ============================================================

def create_product_performance(connection):

    logger.info(
        f"Creating Gold table: {PRODUCT_TABLE}"
    )

    query = f"""
    CREATE OR REPLACE TABLE {PRODUCT_TABLE}
    USING DELTA
    AS

    SELECT

        product_id,
        product_name,

        category,
        sub_category,

        COUNT(*) AS total_events,

        COUNT(
            DISTINCT user_id
        ) AS unique_customers,

        COUNT(
            DISTINCT session_id
        ) AS unique_sessions,

        SUM(
            COALESCE(quantity, 0)
        ) AS total_quantity,

        SUM(
            COALESCE(removed_quantity, 0)
        ) AS total_removed_quantity,

        SUM(
            COALESCE(net_quantity, 0)
        ) AS total_net_quantity,

        SUM(
            COALESCE(sales, 0)
        ) AS total_sales,

        SUM(
            COALESCE(discount_amount, 0)
        ) AS total_discount_amount,

        AVG(
            COALESCE(product_price, 0)
        ) AS average_product_price,

        AVG(
            COALESCE(discount_percent, 0)
        ) AS average_discount_percent

    FROM {SILVER_TABLE_NAME}

    WHERE product_id IS NOT NULL

    GROUP BY
        product_id,
        product_name,
        category,
        sub_category
    """

    with connection.cursor() as cursor:
        cursor.execute(query)

    logger.info(
        "Product performance table created successfully."
    )


# ============================================================
# 3. Customer Activity
# ============================================================

def create_customer_activity(connection):

    logger.info(
        f"Creating Gold table: {CUSTOMER_TABLE}"
    )

    query = f"""
    CREATE OR REPLACE TABLE {CUSTOMER_TABLE}
    USING DELTA
    AS

    SELECT

        user_id,
        MAX(customer_name) AS customer_name,

        MAX(segment) AS segment,

        MAX(country) AS country,
        MAX(market) AS market,
        MAX(region) AS region,

        COUNT(*) AS total_events,

        COUNT(
            DISTINCT session_id
        ) AS total_sessions,

        COUNT(
            DISTINCT product_id
        ) AS unique_products_viewed,

        SUM(
            COALESCE(quantity, 0)
        ) AS total_quantity,

        SUM(
            COALESCE(removed_quantity, 0)
        ) AS total_removed_quantity,

        SUM(
            COALESCE(net_quantity, 0)
        ) AS total_net_quantity,

        SUM(
            COALESCE(sales, 0)
        ) AS total_sales,

        SUM(
            COALESCE(discount_amount, 0)
        ) AS total_discount_amount,

        MIN(event_timestamp) AS first_activity,

        MAX(event_timestamp) AS last_activity

    FROM {SILVER_TABLE_NAME}

    WHERE user_id IS NOT NULL

    GROUP BY user_id
    """

    with connection.cursor() as cursor:
        cursor.execute(query)

    logger.info(
        "Customer activity table created successfully."
    )


# ============================================================
# 4. Conversion Metrics
# ============================================================

def create_conversion_metrics(connection):

    logger.info(
        f"Creating Gold table: {CONVERSION_TABLE}"
    )

    query = f"""
    CREATE OR REPLACE TABLE {CONVERSION_TABLE}
    USING DELTA
    AS

    WITH session_events AS (

        SELECT

            session_id,

            MAX(
                CASE
                    WHEN event_type = 'view'
                    THEN 1
                    ELSE 0
                END
            ) AS has_view,

            MAX(
                CASE
                    WHEN event_type = 'add_to_cart'
                    THEN 1
                    ELSE 0
                END
            ) AS has_add_to_cart,

            MAX(
                CASE
                    WHEN event_type = 'remove_from_cart'
                    THEN 1
                    ELSE 0
                END
            ) AS has_remove_from_cart,

            MAX(
                CASE
                    WHEN event_type IN (
                        'purchase',
                        'order',
                        'checkout',
                        'complete_purchase'
                    )
                    THEN 1
                    ELSE 0
                END
            ) AS has_purchase

        FROM {SILVER_TABLE_NAME}

        WHERE session_id IS NOT NULL

        GROUP BY session_id
    ),

    totals AS (

        SELECT

            COUNT(*) AS total_sessions,

            SUM(has_view) AS view_sessions,

            SUM(has_add_to_cart)
                AS add_to_cart_sessions,

            SUM(has_remove_from_cart)
                AS remove_from_cart_sessions,

            SUM(has_purchase)
                AS purchase_sessions

        FROM session_events
    )

    SELECT

        total_sessions,

        view_sessions,

        add_to_cart_sessions,

        remove_from_cart_sessions,

        purchase_sessions,

        ROUND(
            CASE
                WHEN total_sessions = 0 THEN 0
                ELSE
                    view_sessions * 100.0
                    / total_sessions
            END,
            2
        ) AS view_rate_percent,

        ROUND(
            CASE
                WHEN view_sessions = 0 THEN 0
                ELSE
                    add_to_cart_sessions * 100.0
                    / view_sessions
            END,
            2
        ) AS add_to_cart_rate_percent,

        ROUND(
            CASE
                WHEN add_to_cart_sessions = 0 THEN 0
                ELSE
                    purchase_sessions * 100.0
                    / add_to_cart_sessions
            END,
            2
        ) AS cart_to_purchase_rate_percent,

        ROUND(
            CASE
                WHEN total_sessions = 0 THEN 0
                ELSE
                    purchase_sessions * 100.0
                    / total_sessions
            END,
            2
        ) AS overall_conversion_rate_percent

    FROM totals
    """

    with connection.cursor() as cursor:
        cursor.execute(query)

    logger.info(
        "Conversion metrics table created successfully."
    )


# ============================================================
# Verify Gold tables
# ============================================================

def verify_gold_tables(connection):

    tables = [
        DAILY_SALES_TABLE,
        PRODUCT_TABLE,
        CUSTOMER_TABLE,
        CONVERSION_TABLE,
    ]

    logger.info(
        "=========================================="
    )

    logger.info(
        "VERIFYING GOLD TABLES"
    )

    logger.info(
        "=========================================="
    )

    for table_name in tables:

        with connection.cursor() as cursor:

            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {table_name}
                """
            )

            row_count = cursor.fetchone()[0]

        logger.info(
            f"{table_name} -> {row_count} rows"
        )


# ============================================================
# Main
# ============================================================

def main():

    logger.info(
        "=========================================="
    )

    logger.info(
        "SILVER -> GOLD TRANSFORMATION"
    )

    logger.info(
        "=========================================="
    )

    logger.info(
        f"Silver table: {SILVER_TABLE_NAME}"
    )

    logger.info(
        f"Daily Sales: {DAILY_SALES_TABLE}"
    )

    logger.info(
        f"Product Performance: {PRODUCT_TABLE}"
    )

    logger.info(
        f"Customer Activity: {CUSTOMER_TABLE}"
    )

    logger.info(
        f"Conversion Metrics: {CONVERSION_TABLE}"
    )

    connection = create_sql_connection()

    try:

        # ----------------------------------------------------
        # 1. Verify Silver
        # ----------------------------------------------------

        silver_count = verify_silver_table(
            connection
        )

        # ----------------------------------------------------
        # 2. Create Gold tables
        # ----------------------------------------------------

        create_daily_sales(
            connection
        )

        create_product_performance(
            connection
        )

        create_customer_activity(
            connection
        )

        create_conversion_metrics(
            connection
        )

        # ----------------------------------------------------
        # 3. Verify Gold
        # ----------------------------------------------------

        verify_gold_tables(
            connection
        )

        logger.info(
            f"Silver rows processed: {silver_count}"
        )

        logger.info(
            "=========================================="
        )

        logger.info(
            "GOLD TRANSFORMATION COMPLETED "
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