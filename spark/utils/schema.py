from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)


EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("event_date", StringType(), True),

    StructField("device", StringType(), True),
    StructField("traffic_source", StringType(), True),

    StructField("customer_name", StringType(), True),
    StructField("segment", StringType(), True),

    StructField("country", StringType(), True),
    StructField("market", StringType(), True),
    StructField("region", StringType(), True),

    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("product_name", StringType(), True),

    StructField("product_price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("discount_percent", DoubleType(), True),
    StructField("sales", DoubleType(), True),

    StructField("ship_mode", StringType(), True),
    StructField("order_priority", StringType(), True),

    # Event-specific fields
    StructField("removed_quantity", IntegerType(), True),
    StructField("search_query", StringType(), True),
])