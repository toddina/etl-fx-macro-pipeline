import sys

LOG_PATH = "/Workspace/Users/andreea.todosi@outlook.it/etl-fx-macro-pipeline/databricks_workspace/common"
sys.path.append(LOG_PATH)

from _logging import setup_job_logger

logger = setup_job_logger("bronze_daily")

SRC_PATH = "/Workspace/Users/andreea.todosi@outlook.it/etl-fx-macro-pipeline/databricks_workspace/bronze/src"
sys.path.append(SRC_PATH)

from load_raw_data import load_raw_data

DAILY_INPUT_PATH = "/Volumes/workspace/default/landing_zone/forex_daily_data"
BRONZE_TABLE = "bronze.forex_daily_data"
FILE_TYPE = "json"

try:
    load_raw_data(
        input_path=DAILY_INPUT_PATH,
        bronze_table=BRONZE_TABLE,
        file_type = FILE_TYPE
    )
    logger.info(f"Daily data loaded successfully into {BRONZE_TABLE}")
  
except Exception as e:
    logger.error(f"Error loading daily data from {DAILY_INPUT_PATH}: {e}")
    raise