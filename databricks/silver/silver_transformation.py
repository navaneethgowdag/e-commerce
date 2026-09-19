import os
import sys

from dotenv import load_dotenv
from databricks import sql


# ============================================================
# Project root
# ============================================================

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.abspath(__file__)
        )
    )
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# ============================================================
# Load environment variables
# ============================================================

load_dotenv(
    os.path.join(PROJECT_ROOT, ".env")
)


# ============================================================
# Configuration
# ============================================================

DATABRICKS_HOST = os.getenv(
    "DATABRICKS_HOST"
)

DATABRICKS_SERVER_HOSTNAME = os.getenv(
    "DATABRICKS_SERVER_HOSTNAME"
)

DATABRICKS_HTTP_PATH = os.getenv(
    "DATABRICKS_HTTP_PATH"
)

DATABRICKS_TOKEN = os.getenv(
    "DATABRICKS_TOKEN"
)

CATALOG = os.getenv(
    "DATABRICKS_CATALOG",
    "ecommerce_catalog"
)

SCHEMA = os.getenv(
    "DATABRICKS_SCHEMA",
    "ecommerce"
)

BRONZE_TABLE = os.getenv(
    "DATABRICKS_BRONZE_TABLE",
    "bronze_events"
)

SILVER_TABLE = os.getenv(
    "DATABRICKS_SILVER_TABLE",
    "silver_events"
)


# ============================================================
# Validation
# ============================================================

required_settings = {
    "DATABRICKS_HOST": DATABRICKS_HOST,
    "DATABRICKS_SERVER_HOSTNAME": DATABRICKS_SERVER_HOSTNAME,
    "DATABRICKS_HTTP_PATH": DATABRICKS_HTTP_PATH,
    "DATABRICKS_TOKEN": DATABRICKS_TOKEN,
}

missing = [
    name
    for name, value in required_settings.items()
    if not value
]

if missing:
    raise ValueError(
        "Missing required .env values: "
        + ", ".join(missing)
    )


FULL_BRONZE_TABLE = (
    f"{CATALOG}.{SCHEMA}.{BRONZE_TABLE}"
)

FULL_SILVER_TABLE = (
    f"{CATALOG}.{SCHEMA}.{SILVER_TABLE}"
)


# ============================================================
# Databricks connection
# ============================================================

def get_connection():
    return sql.connect(
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
        catalog=CATALOG,
        schema=SCHEMA,
    )


# ============================================================
# Silver transformation
# ============================================================

def transform_to_silver():

    print("=" * 70)
    print("E-COMMERCE SILVER TRANSFORMATION")
    print("=" * 70)

    print("Bronze table:")
    print(FULL_BRONZE_TABLE)

    print("Silver table:")
    print(FULL_SILVER_TABLE)

    connection = None

    try:
        connection = get_connection()

        print("Connected to Databricks successfully.")

        with connection.cursor() as cursor:

            # ------------------------------------------------
            # Make sure catalog/schema exist
            # ------------------------------------------------

            cursor.execute(
                f"""
                CREATE CATALOG IF NOT EXISTS {CATALOG}
                """
            )

            cursor.execute(
                f"""
                CREATE SCHEMA IF NOT EXISTS
                {CATALOG}.{SCHEMA}
                """
            )

            # ------------------------------------------------
            # Create Silver table
            # ------------------------------------------------

            silver_sql = f"""
            CREATE OR REPLACE TABLE
            {FULL_SILVER_TABLE}
            USING DELTA
            AS

            WITH cleaned AS (

                SELECT

                    -- ------------------------------------------------
                    -- Primary identifiers
                    -- ------------------------------------------------

                    TRIM(event_id) AS event_id,

                    TRIM(user_id) AS user_id,

                    TRIM(product_id) AS product_id,


                    -- ------------------------------------------------
                    -- Standardized event type
                    -- ------------------------------------------------

                    LOWER(
                        TRIM(event_type)
                    ) AS event_type,


                    -- ------------------------------------------------
                    -- Original timestamp
                    -- ------------------------------------------------

                    timestamp,


                    -- ------------------------------------------------
                    -- Existing parsed timestamp
                    -- ------------------------------------------------

                    event_timestamp,

                    event_date,


                    -- ------------------------------------------------
                    -- Numeric fields
                    -- ------------------------------------------------

                    price,

                    quantity,


                    -- ------------------------------------------------
                    -- Device
                    -- ------------------------------------------------

                    LOWER(
                        TRIM(device)
                    ) AS device,


                    -- ------------------------------------------------
                    -- Country
                    -- ------------------------------------------------

                    UPPER(
                        TRIM(country)
                    ) AS country,


                    -- ------------------------------------------------
                    -- Search
                    -- ------------------------------------------------

                    LOWER(
                        TRIM(search_term)
                    ) AS search_term,


                    -- ------------------------------------------------
                    -- Kafka metadata
                    -- ------------------------------------------------

                    topic,

                    partition,

                    offset,

                    kafka_timestamp


                FROM {FULL_BRONZE_TABLE}


                WHERE event_id IS NOT NULL

            ),


            validated AS (

                SELECT

                    event_id,

                    user_id,

                    product_id,

                    event_type,

                    timestamp,

                    event_timestamp,

                    event_date,


                    -- ------------------------------------------------
                    -- Price validation
                    -- ------------------------------------------------

                    CASE

                        WHEN price IS NULL
                            THEN NULL

                        WHEN price < 0
                            THEN NULL

                        ELSE price

                    END AS price,


                    -- ------------------------------------------------
                    -- Quantity validation
                    --
                    -- Transaction events require quantity.
                    -- Non-transaction events keep NULL.
                    -- ------------------------------------------------

                    CASE

                        WHEN event_type IN (
                            'add_to_cart',
                            'remove_from_cart',
                            'purchase',
                            'payment_failed'
                        )

                        THEN

                            CASE

                                WHEN quantity IS NULL
                                    THEN 1

                                WHEN quantity <= 0
                                    THEN 1

                                ELSE quantity

                            END

                        ELSE NULL

                    END AS quantity,


                    device,

                    country,

                    search_term,

                    topic,

                    partition,

                    offset,

                    kafka_timestamp


                FROM cleaned


                WHERE event_type IN (

                    'page_view',
                    'product_view',
                    'search',
                    'add_to_cart',
                    'remove_from_cart',
                    'purchase',
                    'payment_failed'

                )

            ),


            deduplicated AS (

                SELECT *

                FROM (

                    SELECT

                        *,

                        ROW_NUMBER() OVER (

                            PARTITION BY event_id

                            ORDER BY

                                kafka_timestamp DESC,

                                offset DESC

                        ) AS row_num


                    FROM validated

                )

                WHERE row_num = 1

            )


            SELECT

                event_id,

                user_id,

                product_id,

                event_type,

                timestamp,

                event_timestamp,

                event_date,

                price,

                quantity,

                device,

                country,

                search_term,

                topic,

                partition,

                offset,

                kafka_timestamp,


                -- ------------------------------------------------
                -- Derived date fields
                -- ------------------------------------------------

                YEAR(event_date) AS event_year,

                MONTH(event_date) AS event_month,

                DAY(event_date) AS event_day,


                -- ------------------------------------------------
                -- Transaction value
                -- ------------------------------------------------

                CASE

                    WHEN event_type = 'purchase'
                        AND price IS NOT NULL
                        AND quantity IS NOT NULL

                    THEN ROUND(
                        price * quantity,
                        2
                    )

                    ELSE 0.0

                END AS purchase_amount,


                -- ------------------------------------------------
                -- Item value
                -- ------------------------------------------------

                CASE

                    WHEN price IS NOT NULL
                        AND quantity IS NOT NULL

                    THEN ROUND(
                        price * quantity,
                        2
                    )

                    ELSE 0.0

                END AS item_value,


                -- ------------------------------------------------
                -- Processing timestamp
                -- ------------------------------------------------

                CURRENT_TIMESTAMP() AS silver_processed_at


            FROM deduplicated
            """

            print("Running Silver transformation...")

            cursor.execute(silver_sql)

            print("Silver transformation completed.")


            # ------------------------------------------------
            # Count records
            # ------------------------------------------------

            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {FULL_SILVER_TABLE}
                """
            )

            result = cursor.fetchone()

            silver_count = result[0]


            # ------------------------------------------------
            # Event distribution
            # ------------------------------------------------

            cursor.execute(
                f"""
                SELECT
                    event_type,
                    COUNT(*) AS event_count
                FROM {FULL_SILVER_TABLE}
                GROUP BY event_type
                ORDER BY event_count DESC
                """
            )

            event_rows = cursor.fetchall()


            # ------------------------------------------------
            # Sample data
            # ------------------------------------------------

            cursor.execute(
                f"""
                SELECT *
                FROM {FULL_SILVER_TABLE}
                LIMIT 10
                """
            )

            sample_rows = cursor.fetchall()


            # ------------------------------------------------
            # Success
            # ------------------------------------------------

            print("=" * 70)
            print("SILVER TRANSFORMATION SUCCESSFUL")
            print("=" * 70)

            print(
                "Silver table:",
                FULL_SILVER_TABLE
            )

            print(
                "Silver records:",
                silver_count
            )

            print("\nEvent distribution:")

            for row in event_rows:
                print(row)

            print("\nSample rows:")

            for row in sample_rows:
                print(row)


    except Exception as error:

        print("=" * 70)
        print("SILVER TRANSFORMATION FAILED")
        print("=" * 70)

        print(
            f"Error: {error}"
        )

        raise


    finally:

        if connection is not None:
            connection.close()

            print(
                "Databricks connection closed."
            )


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    transform_to_silver()