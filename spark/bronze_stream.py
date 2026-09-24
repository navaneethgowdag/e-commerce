import os

# ============================================================
# Windows / Hadoop environment
# ============================================================

HADOOP_HOME = r"C:\hadoop"
HADOOP_BIN = os.path.join(HADOOP_HOME, "bin")

os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["HADOOP_HOME_DIR"] = HADOOP_HOME
os.environ["hadoop_home_dir"] = HADOOP_HOME
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PATH"] = (
    HADOOP_BIN
    + os.pathsep
    + os.environ.get("PATH", "")
)
os.environ["JAVA_TOOL_OPTIONS"] = (
    "-Dhadoop.home.dir=C:/hadoop"
)


# ============================================================
# Imports
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    to_date,
)

from config.config import config
from spark.utils.schema import EVENT_SCHEMA
from spark.utils.logging_config import get_logger


logger = get_logger("bronze_stream")


# ============================================================
# Spark
# ============================================================

def create_spark_session():

    return (
        SparkSession.builder
        .appName("Ecommerce-Kafka-Bronze")
        .master("local[1]")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
        )
        .getOrCreate()
    )


# ============================================================
# Main
# ============================================================

def main():

    logger.info("==========================================")
    logger.info("E-COMMERCE KAFKA -> SPARK -> BRONZE")
    logger.info("==========================================")

    logger.info(
        f"Kafka bootstrap servers: "
        f"{config.KAFKA_BOOTSTRAP_SERVERS}"
    )

    logger.info(
        f"Kafka topic: "
        f"{config.KAFKA_TOPIC}"
    )

    logger.info(
        f"Bronze output: "
        f"{config.BRONZE_OUTPUT_PATH}"
    )

    logger.info(
        f"Checkpoint: "
        f"{config.SPARK_CHECKPOINT_DIR}"
    )

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        f"Spark version: "
        f"{spark.version}"
    )

    # ========================================================
    # Kafka source
    # ========================================================

    logger.info("Creating Kafka streaming source...")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            config.KAFKA_BOOTSTRAP_SERVERS
        )
        .option(
            "subscribe",
            config.KAFKA_TOPIC
        )
        .option(
            "startingOffsets",
            "earliest"
        )
        .option(
            "failOnDataLoss",
            "false"
        )
        .option(
            "maxOffsetsPerTrigger",
            "10000"
        )
        .load()
    )

    logger.info("Kafka streaming source created.")

    # ========================================================
    # Parse Kafka JSON
    # ========================================================

    parsed_df = (
        kafka_df
        .select(
            col("key")
            .cast("string")
            .alias("kafka_key"),

            col("value")
            .cast("string")
            .alias("json_value"),

            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn(
            "event",
            from_json(
                col("json_value"),
                EVENT_SCHEMA
            )
        )
    )

    # ========================================================
    # Flatten event
    # ========================================================

    bronze_df = (
        parsed_df
        .select(
            "kafka_key",

            "topic",
            "partition",
            "offset",
            "kafka_timestamp",

            "event.*"
        )
        .withColumn(
            "event_timestamp",
            to_timestamp(
                col("timestamp")
            )
        )
        .withColumn(
            "event_date_parsed",
            to_date(
                col("event_date")
            )
        )
    )

    # ========================================================
    # Write Bronze Parquet
    # ========================================================

    logger.info(
        "Starting Bronze streaming query..."
    )

    query = (
        bronze_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option(
            "path",
            config.BRONZE_OUTPUT_PATH
        )
        .option(
            "checkpointLocation",
            config.SPARK_CHECKPOINT_DIR
        )
        .partitionBy(
            "event_date_parsed"
        )
        .trigger(
            processingTime="10 seconds"
        )
        .start()
    )

    logger.info(
        "Bronze streaming query started."
    )

    logger.info(
        "Waiting for Kafka events..."
    )

    try:

        query.awaitTermination()

    except KeyboardInterrupt:

        logger.info(
            "Stopping Bronze streaming query..."
        )

        query.stop()

        logger.info(
            "Bronze streaming query stopped."
        )

    finally:

        spark.stop()

        logger.info(
            "Spark session stopped."
        )


if __name__ == "__main__":
    main()