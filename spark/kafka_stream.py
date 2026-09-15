import os
import sys

# -------------------------------------------------------------------
# Make the project root importable when this file is executed directly.
# Example:
#     python spark\kafka_stream.py
# -------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp

from config.config import config
from spark.utils.logging_config import get_logger
from spark.utils.schema import EVENT_SCHEMA


logger = get_logger("spark_streaming")


def create_spark_session() -> SparkSession:
    """
    Create and configure the local SparkSession.

    Spark uses the Kafka connector to read events from the
    ecommerce-events Kafka topic.
    """

    logger.info("Initializing SparkSession...")

    spark = (
        SparkSession.builder
        .appName("EcommerceKafkaStream")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
        )
        .config(
            "spark.sql.shuffle.partitions",
            "2",
        )
        .getOrCreate()
    )

    # Reduce Spark's default log volume.
    spark.sparkContext.setLogLevel("WARN")

    logger.info("SparkSession initialized successfully.")

    return spark


def create_kafka_stream(spark: SparkSession):
    """
    Create a streaming DataFrame connected to Kafka.
    """

    logger.info(
        f"Connecting to Kafka at "
        f"{config.KAFKA_BOOTSTRAP_SERVERS}..."
    )

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            config.KAFKA_BOOTSTRAP_SERVERS,
        )
        .option(
            "subscribe",
            config.KAFKA_TOPIC,
        )
        .option(
            "startingOffsets",
            "earliest",
        )
        .option(
            "failOnDataLoss",
            "false",
        )
        .load()
    )

    logger.info(
        f"Connected to Kafka topic: {config.KAFKA_TOPIC}"
    )

    return kafka_df


def transform_stream(kafka_df):
    """
    Convert Kafka's binary value into JSON and flatten it
    into normal columns.
    """

    logger.info("Transforming Kafka messages...")

    parsed_df = kafka_df.select(
        col("key")
        .cast("string")
        .alias("kafka_key"),

        from_json(
            col("value").cast("string"),
            EVENT_SCHEMA,
        ).alias("data"),

        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
    )

    flattened_df = parsed_df.select(
        "data.*",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
    )

    # Convert the event timestamp string into Spark TimestampType.
    transformed_df = flattened_df.withColumn(
        "event_timestamp",
        to_timestamp(col("timestamp")),
    )

    return transformed_df


def start_console_stream(transformed_df):
    """
    Start the Structured Streaming query and display events
    in the console.
    """

    logger.info("Starting Spark streaming query...")

    query = (
        transformed_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 10)
        .trigger(processingTime="5 seconds")
        .option(
            "checkpointLocation",
            config.SPARK_CHECKPOINT_DIR,
        )
        .start()
    )

    logger.info("Spark stream started successfully.")
    logger.info("Waiting for Kafka events...")
    logger.info("Press Ctrl+C to stop.")

    return query


def main():
    """
    Main application entry point.
    """

    spark = None
    query = None

    try:
        logger.info("=" * 70)
        logger.info("E-COMMERCE KAFKA -> PYSPARK STREAM")
        logger.info("=" * 70)

        spark = create_spark_session()

        # -----------------------------------------------------------
        # 1. Read from Kafka
        # -----------------------------------------------------------
        kafka_df = create_kafka_stream(spark)

        # -----------------------------------------------------------
        # 2. Parse and transform JSON events
        # -----------------------------------------------------------
        transformed_df = transform_stream(kafka_df)

        # -----------------------------------------------------------
        # 3. Watermark late-arriving events
        # -----------------------------------------------------------
        # We only apply the watermark after converting the timestamp.
        watermarked_df = transformed_df.withWatermark(
            "event_timestamp",
            "10 minutes",
        )

        # -----------------------------------------------------------
        # 4. Start streaming query
        # -----------------------------------------------------------
        query = start_console_stream(watermarked_df)

        # Keep the streaming application alive.
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Spark stream interrupted by user.")

    except Exception as error:
        logger.exception(
            f"Spark streaming application failed: {error}"
        )

    finally:
        if query is not None:
            try:
                query.stop()
                logger.info("Streaming query stopped.")
            except Exception:
                pass

        if spark is not None:
            try:
                spark.stop()
                logger.info("SparkSession stopped.")
            except Exception:
                pass

        logger.info("Application shutdown complete.")


if __name__ == "__main__":
    main()