import os
import sys

# ---------------------------------------------------------------
# Make project root importable.
# ---------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_date,
    to_timestamp,
)

from config.config import config
from config.aws_config import (
    AWS_REGION,
    S3_BRONZE_PATH,
    S3_CHECKPOINT_PATH,
)
from spark.utils.logging_config import get_logger
from spark.utils.schema import EVENT_SCHEMA


logger = get_logger("bronze_stream")


def create_spark_session() -> SparkSession:
    """
    Create SparkSession with Kafka and S3A dependencies.
    """

    logger.info("Initializing SparkSession...")

    spark = (
        SparkSession.builder
        .appName("EcommerceBronzeStream")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
                    "org.apache.hadoop:hadoop-aws:3.5.0",
                ]
            ),
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            AWS_REGION,
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "false",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    logger.info("SparkSession initialized successfully.")

    return spark


def create_kafka_stream(spark: SparkSession):
    """
    Read raw events from Kafka.
    """

    logger.info(
        f"Connecting to Kafka: "
        f"{config.KAFKA_BOOTSTRAP_SERVERS}"
    )

    return (
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


def transform_events(kafka_df):
    """
    Parse Kafka JSON values into structured columns.
    """

    logger.info("Parsing Kafka JSON events...")

    parsed_df = kafka_df.select(
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

    transformed_df = (
        flattened_df
        .withColumn(
            "event_timestamp",
            to_timestamp(col("timestamp")),
        )
        .withColumn(
            "event_date",
            to_date(col("event_timestamp")),
        )
    )

    return transformed_df


def write_to_s3(transformed_df):
    """
    Write Bronze events to S3 using Parquet.
    """

    logger.info(
        f"Bronze destination: {S3_BRONZE_PATH}"
    )

    logger.info(
        f"Checkpoint destination: {S3_CHECKPOINT_PATH}"
    )

    query = (
        transformed_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option(
            "path",
            S3_BRONZE_PATH,
        )
        .option(
            "checkpointLocation",
            S3_CHECKPOINT_PATH,
        )
        .partitionBy("event_date")
        .trigger(processingTime="5 seconds")
        .start()
    )

    return query


def main():
    spark = None
    query = None

    try:
        logger.info("=" * 70)
        logger.info("E-COMMERCE KAFKA -> SPARK -> S3 BRONZE")
        logger.info("=" * 70)

        spark = create_spark_session()

        # 1. Read Kafka.
        kafka_df = create_kafka_stream(spark)

        # 2. Parse/transform events.
        transformed_df = transform_events(kafka_df)

        # 3. Write Bronze to S3.
        query = write_to_s3(transformed_df)

        logger.info(
            "Bronze streaming query started successfully."
        )

        logger.info(
            "Writing Kafka events to AWS S3..."
        )

        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info(
            "Bronze stream interrupted by user."
        )

    except Exception as error:
        logger.exception(
            f"Bronze stream failed: {error}"
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

        logger.info("Bronze application stopped.")


if __name__ == "__main__":
    main()