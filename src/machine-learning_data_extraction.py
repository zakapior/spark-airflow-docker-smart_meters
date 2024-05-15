from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from os import getenv

from constants import (
    FULL_TRANSFORMED_DATASET_INTERIM,
    MACHINE_LEARNING_OUTPUT_PATH,
)

PROCESSING_DATE = getenv("PROCESSING_DATE")

spark = SparkSession.builder.getOrCreate()
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger(__name__)

input_df = (
    spark
    .read
    .parquet(FULL_TRANSFORMED_DATASET_INTERIM)
    .filter(col("day") == PROCESSING_DATE)
)

machine_learning_df = (
    input_df
    .select(
        "lclid",
        "tariff",
        "acorn_group",
        "acorn_group_high_level",
        "date",
        "bank_holidays",
        "weekend",
        "temp_max",
        "temp_min",
        "temp_mean",
        "temp_median",
        "wind_speed_max",
        "wind_speed_min",
        "wind_speed_mean",
        "wind_speed_median",
        "rain",
        "snow",
        "partly-cloudy-night",
        "partly-cloudy-day",
        "fog",
        "cloudy",
        "clear-night",
        "hh_0",
        "hh_1",
        "hh_2",
        "hh_3",
        "hh_4",
        "hh_5",
        "hh_6",
        "hh_7",
        "hh_8",
        "hh_9",
        "hh_10",
        "hh_11",
        "hh_12",
        "hh_13",
        "hh_14",
        "hh_15",
        "hh_16",
        "hh_17",
        "hh_18",
        "hh_19",
        "hh_20",
        "hh_21",
        "hh_22",
        "hh_23",
        "hh_24",
        "hh_25",
        "hh_26",
        "hh_27",
        "hh_28",
        "hh_29",
        "hh_30",
        "hh_31",
        "hh_32",
        "hh_33",
        "hh_34",
        "hh_35",
        "hh_36",
        "hh_37",
        "hh_38",
        "hh_39",
        "hh_40",
        "hh_41",
        "hh_42",
        "hh_43",
        "hh_44",
        "hh_45",
        "hh_46",
        "hh_47",
        "hh_0_tm1",
        "hh_1_tm1",
        "hh_2_tm1",
        "hh_3_tm1",
        "hh_4_tm1",
        "hh_5_tm1",
        "hh_6_tm1",
        "hh_7_tm1",
        "hh_8_tm1",
        "hh_9_tm1",
        "hh_10_tm1",
        "hh_11_tm1",
        "hh_12_tm1",
        "hh_13_tm1",
        "hh_14_tm1",
        "hh_15_tm1",
        "hh_16_tm1",
        "hh_17_tm1",
        "hh_18_tm1",
        "hh_19_tm1",
        "hh_20_tm1",
        "hh_21_tm1",
        "hh_22_tm1",
        "hh_23_tm1",
        "hh_24_tm1",
        "hh_25_tm1",
        "hh_26_tm1",
        "hh_27_tm1",
        "hh_28_tm1",
        "hh_29_tm1",
        "hh_30_tm1",
        "hh_31_tm1",
        "hh_32_tm1",
        "hh_33_tm1",
        "hh_34_tm1",
        "hh_35_tm1",
        "hh_36_tm1",
        "hh_37_tm1",
        "hh_38_tm1",
        "hh_39_tm1",
        "hh_40_tm1",
        "hh_41_tm1",
        "hh_42_tm1",
        "hh_43_tm1",
        "hh_44_tm1",
        "hh_45_tm1",
        "hh_46_tm1",
        "hh_47_tm1",
    )
)

machine_learning_df.write.partitionBy("date").parquet(
    MACHINE_LEARNING_OUTPUT_PATH, mode="overwrite",
)

spark.stop()
