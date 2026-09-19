import os
from dotenv import load_dotenv
from databricks import sql


# ============================================================
# Load environment
# ============================================================

load_dotenv()


# ============================================================
# Databricks configuration
# ============================================================

SERVER_HOSTNAME = os.getenv(
    "DATABRICKS_SERVER_HOSTNAME"
)

HTTP_PATH = os.getenv(
    "DATABRICKS_HTTP_PATH"
)

TOKEN = os.getenv(
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

SILVER_TABLE = (
    f"{CATALOG}.{SCHEMA}.silver_events"
)

GOLD_TABLE = (
    f"{CATALOG}.{SCHEMA}.gold_daily_sales"
)


# ============================================================
# Main
# ============================================================

def create_daily_sales():

    print("=" * 70)
    print("GOLD - DAILY SALES")
    print("=" * 70)

    connection = sql.connect(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        access_token=TOKEN,
        catalog=CATALOG,
        schema=SCHEMA,
    )

    try:

        with connection.cursor() as cursor:

            query = f"""

            CREATE OR REPLACE TABLE {GOLD_TABLE}
            USING DELTA
            AS

            SELECT

                event_date,

                COUNT(*) AS total_events,

                COUNT(
                    CASE
                        WHEN event_type = 'purchase'
                        THEN 1
                    END
                ) AS purchase_events,

                COUNT(
                    CASE
                        WHEN event_type = 'purchase'
                        THEN 1
                    END
                ) AS total_orders,

                COUNT(
                    DISTINCT CASE
                        WHEN event_type = 'purchase'
                        THEN user_id
                    END
                ) AS unique_customers,

                SUM(
                    CASE
                        WHEN event_type = 'purchase'
                        THEN quantity
                        ELSE 0
                    END
                ) AS units_sold,

                ROUND(
                    SUM(
                        CASE
                            WHEN event_type = 'purchase'
                            THEN purchase_amount
                            ELSE 0
                        END
                    ),
                    2
                ) AS total_sales,

                ROUND(
                    AVG(
                        CASE
                            WHEN event_type = 'purchase'
                            THEN purchase_amount
                        END
                    ),
                    2
                ) AS average_order_value,

                CURRENT_TIMESTAMP() AS gold_processed_at

            FROM {SILVER_TABLE}

            GROUP BY event_date

            ORDER BY event_date
            """

            cursor.execute(query)

            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {GOLD_TABLE}
                """
            )

            result = cursor.fetchone()

            print(
                "Gold daily sales rows:",
                result[0]
            )

    finally:
        connection.close()

    print("Daily sales table created successfully.")


if __name__ == "__main__":
    create_daily_sales()