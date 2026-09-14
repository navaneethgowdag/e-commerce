from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType
)

# PySpark schema for our events
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("timestamp", StringType(), False), # Will be cast to Timestamp later
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("device", StringType(), True),
    StructField("country", StringType(), True),
    StructField("search_term", StringType(), True)
])

VALID_EVENT_TYPES = {
    "page_view", "product_view", "search", 
    "add_to_cart", "remove_from_cart", "purchase", "payment_failed"
}