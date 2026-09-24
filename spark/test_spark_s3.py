import os


# ===============================================================
# Windows Hadoop configuration
# ===============================================================

HADOOP_HOME = r"C:\hadoop"
HADOOP_BIN = os.path.join(HADOOP_HOME, "bin")
HADOOP_TMP = os.path.join(HADOOP_HOME, "tmp")

os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["HADOOP_HOME_DIR"] = HADOOP_HOME
os.environ["hadoop_home_dir"] = HADOOP_HOME

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

os.environ["LOCAL_DIRS"] = HADOOP_TMP
os.environ["SPARK_LOCAL_DIRS"] = HADOOP_TMP

os.environ["PATH"] = (
    HADOOP_BIN
    + os.pathsep
    + os.environ.get("PATH", "")
)

os.environ["JAVA_TOOL_OPTIONS"] = (
    "-Dhadoop.home.dir=C:/hadoop"
)


# ===============================================================
# Spark
# ===============================================================

from pyspark.sql import SparkSession


S3_TEST_PATH = (
    "s3a://e-commerce-navaneeth-2026/"
    "spark-test"
)


def main():

    print("=" * 70)
    print("SPARK -> S3 TEST")
    print("=" * 70)

    print("HADOOP_HOME:", os.environ["HADOOP_HOME"])
    print("S3 test path:", S3_TEST_PATH)

    spark = (
        SparkSession.builder
        .appName("SparkS3Test")
        .master("local[1]")

        .config(
            "spark.driver.host",
            "127.0.0.1",
        )

        .config(
            "spark.driver.bindAddress",
            "127.0.0.1",
        )

        .config(
            "spark.local.dir",
            HADOOP_TMP,
        )

        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.5.0",
        )

        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )

        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
        )

        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            "eu-north-1",
        )

        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "s3.eu-north-1.amazonaws.com",
        )

        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "false",
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\nSpark initialized.")

        # -------------------------------------------------------
        # Create tiny test dataset
        # -------------------------------------------------------

        df = spark.createDataFrame(
            [
                (1, "spark", 100.0),
                (2, "s3", 200.0),
                (3, "test", 300.0),
            ],
            [
                "id",
                "name",
                "value",
            ],
        )

        print("\nTest DataFrame:")
        df.show()

        # -------------------------------------------------------
        # Write to S3
        # -------------------------------------------------------

        print(
            "\nWriting Parquet to S3..."
        )

        (
            df.write
            .mode("overwrite")
            .parquet(S3_TEST_PATH)
        )

        print(
            "\nS3 WRITE SUCCESS"
        )

        # -------------------------------------------------------
        # Read from S3
        # -------------------------------------------------------

        print(
            "\nReading Parquet back from S3..."
        )

        result = (
            spark.read
            .parquet(S3_TEST_PATH)
        )

        result.show()

        print(
            "\nS3 READ SUCCESS"
        )

        print(
            "\nRecord count:",
            result.count(),
        )

    except Exception as error:

        print("\n" + "=" * 70)
        print("SPARK -> S3 TEST FAILED")
        print("=" * 70)

        print(error)

        raise

    finally:

        spark.stop()

        print(
            "\nSpark stopped."
        )


if __name__ == "__main__":
    main()