from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from os import getenv

from constants import (
    FULL_TRANSFORMED_DATASET_INTERIM,
    DATA_ANALYTICS_OUTPUT_PATH,
)

PROCESSING_DATE = getenv("PROCESSING_DATE")

spark = SparkSession.builder.getOrCreate()
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger(__name__)

input_df = (
    spark
    .read
    .parquet(FULL_TRANSFORMED_DATASET_INTERIM)
    .filter(col("date") == PROCESSING_DATE)
)

analytics_df = (
    input_df
    .orderBy("LCLid", "date")
    .select(
        input_df.lclid,
        input_df.tariff,
        input_df.acorn_group,
        input_df.acorn_group_high_level,
        input_df.date,
        input_df.week_no,
        input_df.month_no,
        input_df.bank_holidays,
        input_df.weekend,
        input_df.hh_0,
        input_df.hh_1,
        input_df.hh_2,
        input_df.hh_3,
        input_df.hh_4,
        input_df.hh_5,
        input_df.hh_6,
        input_df.hh_7,
        input_df.hh_8,
        input_df.hh_9,
        input_df.hh_10,
        input_df.hh_11,
        input_df.hh_12,
        input_df.hh_13,
        input_df.hh_14,
        input_df.hh_15,
        input_df.hh_16,
        input_df.hh_17,
        input_df.hh_18,
        input_df.hh_19,
        input_df.hh_20,
        input_df.hh_21,
        input_df.hh_22,
        input_df.hh_23,
        input_df.hh_24,
        input_df.hh_25,
        input_df.hh_26,
        input_df.hh_27,
        input_df.hh_28,
        input_df.hh_29,
        input_df.hh_30,
        input_df.hh_31,
        input_df.hh_32,
        input_df.hh_33,
        input_df.hh_34,
        input_df.hh_35,
        input_df.hh_36,
        input_df.hh_37,
        input_df.hh_38,
        input_df.hh_39,
        input_df.hh_40,
        input_df.hh_41,
        input_df.hh_42,
        input_df.hh_43,
        input_df.hh_44,
        input_df.hh_45,
        input_df.hh_46,
        input_df.hh_47,
    )
)

analytics_df.write.partitionBy("date").parquet(
    DATA_ANALYTICS_OUTPUT_PATH, mode="overwrite"
)

spark.stop()
