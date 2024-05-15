"""
Constants with filesystem paths for the project.

The data/ catalog from the host is mapped to /opt/spark/work-dir/data/ folder
within the Docker container. All the input datasets reside within data/input
and the output files reside in data/output catalog.
"""

DATA_PATH = "/opt/spark/work-dir/data"
INPUT_PATH = f"{DATA_PATH}/raw"
INTERIM_PATH = f"{DATA_PATH}/interim"
OUTPUT_PATH = f"{DATA_PATH}/output"

HALF_HOURLY_DATASET_INPUT = f"{INPUT_PATH}/hhblock_dataset/hhblock_dataset"
HOUSEHOLD_INFO_INPUT = f"{INPUT_PATH}/informations_households.csv"
BANK_HOLIDAYS_INPUT = f"{INPUT_PATH}/uk_bank_holidays.csv"
WEATHER_DAILY_INPUT = f"{INPUT_PATH}/weather_daily_darksky.csv"
WEATHER_HOURLY_INPUT = f"{INPUT_PATH}/weather_hourly_darksky.csv"

MACHINE_LEARNING_OUTPUT_PATH = f"{OUTPUT_PATH}/machine-learning-dataset"
DATA_ANALYTICS_OUTPUT_PATH = f"{OUTPUT_PATH}/data-analytics-dataset"

HALF_HOURLY_DATASET_INTERIM = f"{INTERIM_PATH}/half-hourly-dataset"
FULL_TRANSFORMED_DATASET_INTERIM = f"{INTERIM_PATH}/full_smart_meters_dataset"
