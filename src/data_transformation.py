from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    weekofyear,
    month,
    when,
    weekday,
    lag,
    col,
    to_date,
    max,
    min,
    mean,
    median,
    count,
    broadcast,
)
from pyspark.sql.window import Window
from pyspark.sql.types import BooleanType
from os import getenv

from constants import (
    HOUSEHOLD_INFO_INPUT,
    BANK_HOLIDAYS_INPUT,
    HALF_HOURLY_DATASET_INTERIM,
    WEATHER_HOURLY_INPUT,
    FULL_TRANSFORMED_DATASET_INTERIM,
)

PROCESSING_DATE = getenv("PROCESSING_DATE")

spark = SparkSession.builder.getOrCreate()
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger(__name__)

log.info("SparkSession created")


halfhourly_usage_df = (
    spark.read.options(header=True)
    .options(inferSchema=True)
    .csv(HALF_HOURLY_DATASET_INTERIM)
    .filter(col("day") == PROCESSING_DATE)
)

household_info_df = (
    spark.read.options(header=True).options(inferSchema=True).csv(HOUSEHOLD_INFO_INPUT)
)

bank_holidays_df = (
    spark.read.options(header=True)
    .options(inferSchema=True)
    .csv(BANK_HOLIDAYS_INPUT)
    .filter(col("Bank holidays") == PROCESSING_DATE)
)

weather_df = (
    spark.read.options(header=True)
    .options(inferSchema=True)
    .csv(WEATHER_HOURLY_INPUT)
    .filter(to_date(col("time")) == PROCESSING_DATE)
    .groupBy(to_date(col("time")))
    .agg(
        max("temperature").alias("temp_max"),
        min("temperature").alias("temp_min"),
        mean("temperature").alias("temp_mean"),
        median("temperature").alias("temp_median"),
        max("windSpeed").alias("wind_speed_max"),
        min("windSpeed").alias("wind_speed_min"),
        mean("windSpeed").alias("wind_speed_mean"),
        median("windSpeed").alias("wind_speed_median"),
        count(when(col("icon") == "rain", 1)).cast(BooleanType()).alias("rain"),
        count(when(col("icon") == "snow", 1)).cast(BooleanType()).alias("snow"),
        count(when(col("icon") == "partly-cloudy-night", 1))
        .cast(BooleanType())
        .alias("partly-cloudy-night"),
        count(when(col("icon") == "partly-cloudy-day", 1))
        .cast(BooleanType())
        .alias("partly-cloudy-day"),
        count(when(col("icon") == "fog", 1)).cast(BooleanType()).alias("fog"),
        count(when(col("icon") == "cloudy", 1)).cast(BooleanType()).alias("cloudy"),
        count(when(col("icon") == "clear-night", 1))
        .cast(BooleanType())
        .alias("clear-night"),
    )
    .withColumnRenamed("to_date(time)", "day")
)

window = Window().partitionBy("lclid").orderBy("date")

df = (
    halfhourly_usage_df.join(household_info_df, "LCLid", "left")
    .join(
        broadcast(bank_holidays_df),
        bank_holidays_df["Bank holidays"] == halfhourly_usage_df.day,
        "left",
    )
    .join(weather_df, "day", "left")
    .orderBy("LCLid", "day")
    .select(
        halfhourly_usage_df.LCLid.alias("lclid"),
        household_info_df.stdorToU.alias("tariff"),
        household_info_df.Acorn.alias("acorn_group"),
        household_info_df.Acorn_grouped.alias("acorn_group_high_level"),
        halfhourly_usage_df.day.alias("date"),
        weekofyear(halfhourly_usage_df.day).alias("week_no"),
        month(halfhourly_usage_df.day).alias("month_no"),
        when(halfhourly_usage_df.day.isin(bank_holidays_df["Bank holidays"]), True)
        .otherwise(False)
        .alias("bank_holidays"),
        when(weekday(halfhourly_usage_df.day) == 5, True)
        .when(weekday(halfhourly_usage_df.day) == 6, True)
        .otherwise(False)
        .alias("weekend"),
        weather_df["*"],
        halfhourly_usage_df.hh_0,
        halfhourly_usage_df.hh_1,
        halfhourly_usage_df.hh_2,
        halfhourly_usage_df.hh_3,
        halfhourly_usage_df.hh_4,
        halfhourly_usage_df.hh_5,
        halfhourly_usage_df.hh_6,
        halfhourly_usage_df.hh_7,
        halfhourly_usage_df.hh_8,
        halfhourly_usage_df.hh_9,
        halfhourly_usage_df.hh_10,
        halfhourly_usage_df.hh_11,
        halfhourly_usage_df.hh_12,
        halfhourly_usage_df.hh_13,
        halfhourly_usage_df.hh_14,
        halfhourly_usage_df.hh_15,
        halfhourly_usage_df.hh_16,
        halfhourly_usage_df.hh_17,
        halfhourly_usage_df.hh_18,
        halfhourly_usage_df.hh_19,
        halfhourly_usage_df.hh_20,
        halfhourly_usage_df.hh_21,
        halfhourly_usage_df.hh_22,
        halfhourly_usage_df.hh_23,
        halfhourly_usage_df.hh_24,
        halfhourly_usage_df.hh_25,
        halfhourly_usage_df.hh_26,
        halfhourly_usage_df.hh_27,
        halfhourly_usage_df.hh_28,
        halfhourly_usage_df.hh_29,
        halfhourly_usage_df.hh_30,
        halfhourly_usage_df.hh_31,
        halfhourly_usage_df.hh_32,
        halfhourly_usage_df.hh_33,
        halfhourly_usage_df.hh_34,
        halfhourly_usage_df.hh_35,
        halfhourly_usage_df.hh_36,
        halfhourly_usage_df.hh_37,
        halfhourly_usage_df.hh_38,
        halfhourly_usage_df.hh_39,
        halfhourly_usage_df.hh_40,
        halfhourly_usage_df.hh_41,
        halfhourly_usage_df.hh_42,
        halfhourly_usage_df.hh_43,
        halfhourly_usage_df.hh_44,
        halfhourly_usage_df.hh_45,
        halfhourly_usage_df.hh_46,
        halfhourly_usage_df.hh_47,
    )
    .withColumn("hh_0_tm1", lag("hh_0").over(window))
    .withColumn("hh_1_tm1", lag("hh_1").over(window))
    .withColumn("hh_2_tm1", lag("hh_2").over(window))
    .withColumn("hh_3_tm1", lag("hh_3").over(window))
    .withColumn("hh_4_tm1", lag("hh_4").over(window))
    .withColumn("hh_5_tm1", lag("hh_5").over(window))
    .withColumn("hh_6_tm1", lag("hh_6").over(window))
    .withColumn("hh_7_tm1", lag("hh_7").over(window))
    .withColumn("hh_8_tm1", lag("hh_8").over(window))
    .withColumn("hh_9_tm1", lag("hh_9").over(window))
    .withColumn("hh_10_tm1", lag("hh_10").over(window))
    .withColumn("hh_11_tm1", lag("hh_11").over(window))
    .withColumn("hh_12_tm1", lag("hh_12").over(window))
    .withColumn("hh_13_tm1", lag("hh_13").over(window))
    .withColumn("hh_14_tm1", lag("hh_14").over(window))
    .withColumn("hh_15_tm1", lag("hh_15").over(window))
    .withColumn("hh_16_tm1", lag("hh_16").over(window))
    .withColumn("hh_17_tm1", lag("hh_17").over(window))
    .withColumn("hh_18_tm1", lag("hh_18").over(window))
    .withColumn("hh_19_tm1", lag("hh_19").over(window))
    .withColumn("hh_20_tm1", lag("hh_20").over(window))
    .withColumn("hh_21_tm1", lag("hh_21").over(window))
    .withColumn("hh_22_tm1", lag("hh_22").over(window))
    .withColumn("hh_23_tm1", lag("hh_23").over(window))
    .withColumn("hh_24_tm1", lag("hh_24").over(window))
    .withColumn("hh_25_tm1", lag("hh_25").over(window))
    .withColumn("hh_26_tm1", lag("hh_26").over(window))
    .withColumn("hh_27_tm1", lag("hh_27").over(window))
    .withColumn("hh_28_tm1", lag("hh_28").over(window))
    .withColumn("hh_29_tm1", lag("hh_29").over(window))
    .withColumn("hh_30_tm1", lag("hh_30").over(window))
    .withColumn("hh_31_tm1", lag("hh_31").over(window))
    .withColumn("hh_32_tm1", lag("hh_32").over(window))
    .withColumn("hh_33_tm1", lag("hh_33").over(window))
    .withColumn("hh_34_tm1", lag("hh_34").over(window))
    .withColumn("hh_35_tm1", lag("hh_35").over(window))
    .withColumn("hh_36_tm1", lag("hh_36").over(window))
    .withColumn("hh_37_tm1", lag("hh_37").over(window))
    .withColumn("hh_38_tm1", lag("hh_38").over(window))
    .withColumn("hh_39_tm1", lag("hh_39").over(window))
    .withColumn("hh_40_tm1", lag("hh_40").over(window))
    .withColumn("hh_41_tm1", lag("hh_41").over(window))
    .withColumn("hh_42_tm1", lag("hh_42").over(window))
    .withColumn("hh_43_tm1", lag("hh_43").over(window))
    .withColumn("hh_44_tm1", lag("hh_44").over(window))
    .withColumn("hh_45_tm1", lag("hh_45").over(window))
    .withColumn("hh_46_tm1", lag("hh_46").over(window))
    .withColumn("hh_47_tm1", lag("hh_47").over(window))
)

onehot_acorn = [
    group[0]
    for group in df.select(col("acorn_group")).distinct().collect()
]

for group in onehot_acorn:
    df = df.withColumn(
        group, when((df["acorn_group"] == group), 1).otherwise(0).cast(BooleanType())
    )

df.printSchema()

df.write.partitionBy("date").parquet(FULL_TRANSFORMED_DATASET_INTERIM, mode="overwrite")

spark.stop()
