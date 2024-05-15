"""
The one-shot script to transform the input dataset, that is rather
unstructured, into elegant, partitioned by date set of CSV files.

It is meant to be run within a Docker Spark container.
"""

from pyspark.sql import SparkSession
from constants import HALF_HOURLY_DATASET_INPUT, HALF_HOURLY_DATASET_INTERIM

spark = SparkSession.builder.getOrCreate()
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger(__name__)

try:
    log.info(f"Loading dataset from {HALF_HOURLY_DATASET_INPUT}")

    halfhourly_usage_df = (
        spark
        .read
        .options(header=True)
        .options(inferSchema=True)
        .csv(HALF_HOURLY_DATASET_INPUT)
    )

except Exception as e:
    log.error(
        f"An error occured while loading dataset from {HALF_HOURLY_DATASET_INPUT}: {e}"
    )

try:
    log.info(f"Writing output to {HALF_HOURLY_DATASET_INTERIM}")

    (
        halfhourly_usage_df
        .write
        .option("header", True)
        .partitionBy("day")
        .csv(HALF_HOURLY_DATASET_INTERIM, mode="overwrite")
    )

except Exception as e:
    log.error(
        f"An error occured while writing output to {HALF_HOURLY_DATASET_INTERIM}: {e}"
    )

spark.stop()
