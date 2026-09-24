import os
import sys


# ===============================================================
# Project root
# ===============================================================

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# ===============================================================
# Configure Windows Hadoop BEFORE importing PySpark
# ===============================================================

from spark.utils.windows_spark import (
    configure_windows_spark,
)

configure_windows_spark()


# ===============================================================
# PySpark imports
# ===============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
)


# ===============================================================
# Project imports
# ===============================================================

from config.config import config

from spark.utils.logging_config import (
    get_logger,
)

from spark.utils.schema import (
    EVENT_SCHEMA,
)


# ===============================================================
# Logger
# ===============================================================

logger = get_logger(
    "spark_streaming"
)


# ===============================================================
# Spark session
# ===============================================================

def create_spark_session() -> SparkSession:
    """
    Create and configure the local SparkSession.
    """

    logger.info(
        "Initializing SparkSession..."
    )

    logger.info(
        f"HADOOP_HOME: "
        f"{os.environ.get('HADOOP_HOME')}"
    )

    logger.info(
        f"SPARK_LOCAL_IP: "
        f"{os.environ.get('SPARK_LOCAL_IP')}"
    )

    spark = (
        SparkSession.builder

        .appName(
            "EcommerceKafkaStream"
        )

        .master(
            "local[1]"
        )

        # -----------------------------------------------
        # Local Windows networking
        # -----------------------------------------------

        .config(
            "spark.driver.host",
            "127.0.0.1",
        )

        .config(
            "spark.driver.bindAddress",
            "127.0.0.1",
        )

        # -----------------------------------------------
        # Kafka connector
        # -----------------------------------------------

        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
        )

        # -----------------------------------------------
        # Spark settings
        # -----------------------------------------------

        .config(
            "spark.sql.shuffle.partitions",
            "2",
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    logger.info(
        "SparkSession initialized successfully."
    )

    return spark


# ===============================================================
# Kafka stream
# ===============================================================

def create_kafka_stream(
    spark: SparkSession,
):
    """
    Create a streaming DataFrame connected to Kafka.
    """

    logger.info(
        f"Connecting to Kafka at "
        f"{config.KAFKA_BOOTSTRAP_SERVERS}..."
    )

    kafka_df = (
        spark.readStream

        .format(
            "kafka"
        )

        .option(
            "kafka.bootstrap.servers",
            config.KAFKA_BOOTSTRAP_SERVERS,
        )

        .option(
            "subscribe",
            config.KAFKA_TOPIC,
        )

        # ------------------------------------------------
        # You already have ~150,000 records in Kafka.
        # ------------------------------------------------

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
        f"Connected to Kafka topic: "
        f"{config.KAFKA_TOPIC}"
    )

    return kafka_df


# ===============================================================
# Transform Kafka events
# ===============================================================

def transform_stream(
    kafka_df,
):
    """
    Convert Kafka JSON values into structured columns.
    """

    logger.info(
        "Transforming Kafka messages..."
    )

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

        col("timestamp").alias(
            "kafka_timestamp"
        ),
    )

    flattened_df = parsed_df.select(
        "data.*",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
    )

    transformed_df = (
        flattened_df

        .withColumn(
            "event_timestamp",
            to_timestamp(
                col("timestamp")
            ),
        )
    )

    return transformed_df


# ===============================================================
# Console streaming
# ===============================================================

def start_console_stream(
    transformed_df,
):
    """
    Start Structured Streaming and display events.
    """

    logger.info(
        "Starting Spark streaming query..."
    )

    query = (
        transformed_df.writeStream

        .format(
            "console"
        )

        .outputMode(
            "append"
        )

        .option(
            "truncate",
            "false",
        )

        .option(
            "numRows",
            10,
        )

        .trigger(
            processingTime="5 seconds"
        )

        .option(
            "checkpointLocation",
            config.SPARK_CHECKPOINT_DIR,
        )

        .start()
    )

    logger.info(
        "Spark stream started successfully."
    )

    logger.info(
        "Waiting for Kafka events..."
    )

    logger.info(
        "Press Ctrl+C to stop."
    )

    return query


# ===============================================================
# Main
# ===============================================================

def main():

    spark = None
    query = None

    try:

        logger.info(
            "=" * 70
        )

        logger.info(
            "E-COMMERCE KAFKA -> PYSPARK STREAM"
        )

        logger.info(
            "=" * 70
        )

        spark = (
            create_spark_session()
        )

        kafka_df = (
            create_kafka_stream(
                spark
            )
        )

        transformed_df = (
            transform_stream(
                kafka_df
            )
        )

        watermarked_df = (
            transformed_df.withWatermark(
                "event_timestamp",
                "10 minutes",
            )
        )

        query = (
            start_console_stream(
                watermarked_df
            )
        )

        query.awaitTermination()

    except KeyboardInterrupt:

        logger.info(
            "Spark stream interrupted by user."
        )

    except Exception as error:

        logger.exception(
            f"Spark streaming application failed: "
            f"{error}"
        )

    finally:

        if query is not None:

            try:
                query.stop()
            except Exception:
                pass

        if spark is not None:

            try:
                spark.stop()
            except Exception:
                pass

        logger.info(
            "Application shutdown complete."
        )


if __name__ == "__main__":
    main()