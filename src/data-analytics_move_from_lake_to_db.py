"""
This script is intended to be used as a second step after transforming the
input dataset into the Parquet files. It takes the generated Parquet file and
loads contents in the PostgreSQL database.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from os import getenv
from constants import DATA_ANALYTICS_OUTPUT_PATH

PROCESSING_DATE = getenv("PROCESSING_DATE")

DB_URL = f"jdbc:postgresql://{getenv('DB_HOST')}:{getenv('DB_PORT')}/{getenv('DB_DATABASE')}"
DB_USER = getenv("DB_USER")
DB_PASSWORD = getenv("DB_PASSWORD")
DB_DRIVER = getenv("DB_DRIVER")
DB_TABLE = getenv("DB_TABLE")

spark = SparkSession.builder.getOrCreate()
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger(__name__)

data_analytics_df = spark.read.parquet(DATA_ANALYTICS_OUTPUT_PATH).filter(
    col("date") == PROCESSING_DATE
)

(
    data_analytics_df.write.format("jdbc")
    .option("url", DB_URL)
    .option("driver", DB_DRIVER)
    .option("dbtable", DB_TABLE)
    .option("user", DB_USER)
    .option("password", DB_PASSWORD)
    .mode("overwrite")
    .save()
)
