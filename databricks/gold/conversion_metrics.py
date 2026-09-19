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
    f"{CATALOG}.{SCHEMA}.gold_conversion_metrics"
)


def create_conversion_metrics():

    print("=" * 70)
    print("GOLD - CONVERSION METRICS")
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

            WITH daily_funnel AS (

                SELECT

                    event_date,

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
                    ) AS purchases

                FROM {SILVER_TABLE}

                GROUP BY event_date
            )


            SELECT

                event_date,

                page_views,

                product_views,

                add_to_cart_events,

                purchases,


                -- ------------------------------------------------
                -- Page view -> product view
                -- ------------------------------------------------

                ROUND(
                    CASE
                        WHEN page_views > 0
                        THEN (
                            product_views * 100.0
                            / page_views
                        )
                        ELSE 0
                    END,
                    2
                ) AS product_view_rate,


                -- ------------------------------------------------
                -- Product view -> cart
                -- ------------------------------------------------

                ROUND(
                    CASE
                        WHEN product_views > 0
                        THEN (
                            add_to_cart_events * 100.0
                            / product_views
                        )
                        ELSE 0
                    END,
                    2
                ) AS add_to_cart_rate,


                -- ------------------------------------------------
                -- Cart -> purchase
                -- ------------------------------------------------

                ROUND(
                    CASE
                        WHEN add_to_cart_events > 0
                        THEN (
                            purchases * 100.0
                            / add_to_cart_events
                        )
                        ELSE 0
                    END,
                    2
                ) AS purchase_rate,


                -- ------------------------------------------------
                -- Overall page -> purchase
                -- ------------------------------------------------

                ROUND(
                    CASE
                        WHEN page_views > 0
                        THEN (
                            purchases * 100.0
                            / page_views
                        )
                        ELSE 0
                    END,
                    2
                ) AS overall_conversion_rate,


                CURRENT_TIMESTAMP() AS gold_processed_at


            FROM daily_funnel

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
                "Conversion rows:",
                result[0]
            )

    finally:
        connection.close()

    print(
        "Conversion metrics table created successfully."
    )


if __name__ == "__main__":
    create_conversion_metrics()