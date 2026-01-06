import sys

LOG_PATH = "/Workspace/Users/andreea.todosi@outlook.it/etl-fx-macro-pipeline/databricks_workspace/common"
sys.path.append(LOG_PATH)

from _logging import setup_job_logger

logger = setup_job_logger("bronze_historical")

SRC_PATH = "/Workspace/Users/andreea.todosi@outlook.it/etl-fx-macro-pipeline/databricks_workspace/bronze/src"
sys.path.append(SRC_PATH)

from load_raw_data import load_raw_data

HISTORICAL_INPUT_PATH = "/Volumes/workspace/default/landing_zone/forex_historical_data"
BRONZE_TABLE = "bronze.forex_historical_data"
FILE_TYPE = "json"

try:
    load_raw_data(
        input_path=HISTORICAL_INPUT_PATH, 
        bronze_table=BRONZE_TABLE,
        file_type = FILE_TYPE,
        mode="overwrite"
    )
    logger.info(f"Historical data loaded successfully into {BRONZE_TABLE}")
  
except Exception as e:
    logger.error(f"Error loading historical data from {HISTORICAL_INPUT_PATH}: {e}")
    raise