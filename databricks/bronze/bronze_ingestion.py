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

S3_BRONZE_PATH = os.getenv(
    "S3_BRONZE_PATH"
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


# ============================================================
# Validate configuration
# ============================================================

required_settings = {
    "DATABRICKS_HOST": DATABRICKS_HOST,
    "DATABRICKS_SERVER_HOSTNAME": DATABRICKS_SERVER_HOSTNAME,
    "DATABRICKS_HTTP_PATH": DATABRICKS_HTTP_PATH,
    "DATABRICKS_TOKEN": DATABRICKS_TOKEN,
    "S3_BRONZE_PATH": S3_BRONZE_PATH,
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


FULL_TABLE_NAME = (
    f"{CATALOG}.{SCHEMA}.{BRONZE_TABLE}"
)


# ============================================================
# Connect to Databricks
# ============================================================

def get_connection():
    """
    Create a Databricks SQL connection.
    """

    return sql.connect(
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
        catalog=CATALOG,
        schema=SCHEMA,
    )


# ============================================================
# Bronze ingestion
# ============================================================

def ingest_bronze():
    """
    Register the S3 Bronze Parquet data as a Databricks
    external table.
    """

    print("=" * 70)
    print("E-COMMERCE BRONZE INGESTION")
    print("=" * 70)

    print(
        "Databricks host:",
        DATABRICKS_HOST
    )

    print(
        "S3 Bronze path:",
        S3_BRONZE_PATH
    )

    print(
        "Target table:",
        FULL_TABLE_NAME
    )

    connection = None

    try:
        # ----------------------------------------------------
        # Connect
        # ----------------------------------------------------

        connection = get_connection()

        print(
            "Connected to Databricks successfully."
        )

        with connection.cursor() as cursor:

            # ------------------------------------------------
            # Create catalog
            # ------------------------------------------------

            cursor.execute(
                f"""
                CREATE CATALOG IF NOT EXISTS {CATALOG}
                """
            )

            # ------------------------------------------------
            # Create schema
            # ------------------------------------------------

            cursor.execute(
                f"""
                CREATE SCHEMA IF NOT EXISTS
                {CATALOG}.{SCHEMA}
                """
            )

            # ------------------------------------------------
            # Create external Bronze table
            # ------------------------------------------------

            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS
            {FULL_TABLE_NAME}
            USING PARQUET
            LOCATION '{S3_BRONZE_PATH}'
            """

            print(
                "Creating Bronze table..."
            )

            cursor.execute(
                create_table_sql
            )

            # ------------------------------------------------
            # Count records
            # ------------------------------------------------

            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {FULL_TABLE_NAME}
                """
            )

            result = cursor.fetchone()

            record_count = result[0]

            # ------------------------------------------------
            # Show sample data
            # ------------------------------------------------

            cursor.execute(
                f"""
                SELECT *
                FROM {FULL_TABLE_NAME}
                LIMIT 10
                """
            )

            rows = cursor.fetchall()

            # ------------------------------------------------
            # Success
            # ------------------------------------------------

            print("=" * 70)
            print(
                "BRONZE INGESTION SUCCESSFUL"
            )
            print("=" * 70)

            print(
                "Table:",
                FULL_TABLE_NAME
            )

            print(
                "Records:",
                record_count
            )

            print(
                "\nSample rows:"
            )

            for row in rows:
                print(row)

    except Exception as error:

        print("=" * 70)
        print("BRONZE INGESTION FAILED")
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
    ingest_bronze()