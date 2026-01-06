import sys

LOG_PATH = "/Workspace/Users/andreea.todosi@outlook.it/etl-fx-macro-pipeline/databricks_workspace/common"
sys.path.append(LOG_PATH)

from _logging import setup_job_logger

logger = setup_job_logger("bronze_monthly")

SRC_PATH = "/Workspace/Users/andreea.todosi@outlook.it/etl-fx-macro-pipeline/databricks_workspace/bronze/src"
sys.path.append(SRC_PATH)

from load_raw_data import load_raw_data

MONTHLY_INPUT_PATH = "/Volumes/workspace/default/landing_zone/macro_finance_monthly_data"
BRONZE_TABLE = "bronze.macro_finance_monthly_data"
FILE_TYPE = "parquet"

try:
    load_raw_data(
        input_path=MONTHLY_INPUT_PATH,
        bronze_table=BRONZE_TABLE,
        file_type = FILE_TYPE
    )
    logger.info(f"Monthly data loaded successfully into {BRONZE_TABLE}")

except Exception as e:
    logger.error(f"Error loading daily data from {MONTHLY_INPUT_PATH}: {e}")
    raise