import os

# Windows / Hadoop configuration
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["HADOOP_HOME_DIR"] = "C:/hadoop"
os.environ["hadoop_home_dir"] = "C:/hadoop"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
os.environ["PATH"] = "C:/hadoop/bin;" + os.environ.get("PATH", "")
os.environ["JAVA_TOOL_OPTIONS"] = "-Dhadoop.home.dir=C:/hadoop"

from pyspark.sql import SparkSession

print("=" * 70)
print("SPARK + SPARK-HADOOP-CLOUD TEST")
print("=" * 70)

spark = (
    SparkSession.builder
    .appName("HadoopAWSTest")
    .master("local[1]")
    .config("spark.driver.host", "localhost")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.ui.enabled", "false")

    # Use a fresh local Ivy cache
    .config("spark.jars.ivy", "C:/spark-ivy")

    # Spark 4.2.0 cloud integration
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-hadoop-cloud_2.13:4.2.0"
    )
    .getOrCreate()
)

try:
    print()
    print("SPARK + SPARK-HADOOP-CLOUD WORKS")
    print("Count:", spark.range(10).count())
finally:
    spark.stop()