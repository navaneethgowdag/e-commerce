import os
from dotenv import load_dotenv
from databricks import sql


load_dotenv()


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
    f"{CATALOG}.{SCHEMA}.gold_customer_activity"
)


def create_customer_activity():

    print("=" * 70)
    print("GOLD - CUSTOMER ACTIVITY")
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

                user_id,

                COUNT(*) AS total_events,

                COUNT(
                    DISTINCT event_date
                ) AS active_days,

                COUNT(
                    DISTINCT event_type
                ) AS event_types_used,

                COUNT(
                    DISTINCT product_id
                ) AS products_interacted,

                COUNT(
                    CASE
                        WHEN event_type = 'page_view'
                        THEN 1
                    END
                ) AS page_views,

                COUNT(
                    CASE
                        WHEN event_type = 'product_view'
                        THEN 1
                    END
                ) AS product_views,

                COUNT(
                    CASE
                        WHEN event_type = 'add_to_cart'
                        THEN 1
                    END
                ) AS add_to_cart_events,

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
                ) AS orders,

                SUM(
                    CASE
                        WHEN event_type = 'purchase'
                        THEN quantity
                        ELSE 0
                    END
                ) AS units_purchased,

                ROUND(
                    SUM(
                        CASE
                            WHEN event_type = 'purchase'
                            THEN purchase_amount
                            ELSE 0
                        END
                    ),
                    2
                ) AS total_spend,

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

            WHERE user_id IS NOT NULL

            GROUP BY user_id
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
                "Customer rows:",
                result[0]
            )

    finally:
        connection.close()

    print(
        "Customer activity table created successfully."
    )


if __name__ == "__main__":
    create_customer_activity()