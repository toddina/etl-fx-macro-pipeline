import sys

LOG_PATH = "/Workspace/Users/andreea.todosi@outlook.it/etl-fx-macro-pipeline/databricks_workspace/common"
sys.path.append(LOG_PATH)

from _logging import setup_job_logger

logger = setup_job_logger("silver_historical")

SRC_PATH = "/Workspace/Users/andreea.todosi@outlook.it/etl-fx-macro-pipeline/databricks_workspace/silver/src"
sys.path.append(SRC_PATH)

from transform_rates import transform_rates

BRONZE_TABLE = "bronze.forex_historical_data"
SILVER_TABLE = "silver.exchange_rates"

raw_df = spark.read.table(BRONZE_TABLE)

transformed_df = transform_rates(raw_df)

(
    transformed_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("exchange_date")
    .saveAsTable(SILVER_TABLE)
)

logger.info(f"Silver historical table {SILVER_TABLE} created")
