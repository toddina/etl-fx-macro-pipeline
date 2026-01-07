import sys, os
from upload_utils import upload_to_databricks
from datetime import date

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.paths import SOURCES_CONFIG_PATH
from utils.config_loader import load_sources_config

config = load_sources_config(SOURCES_CONFIG_PATH)["databricks"]

FILE_NAME = f"{str(date.today())}.parquet"
MONTHLY_MACRO_FINANCE_VOLUME_PATH = f"{config["path_volume"]["monthly"]}{FILE_NAME}"

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

MONTHLY_MACRO_FINANCE_LOCAL_PATH = f"monthly_macro_finance/{FILE_NAME}"

if __name__ == "__main__":
    upload_to_databricks(local_path=MONTHLY_MACRO_FINANCE_LOCAL_PATH, volume_path=MONTHLY_MACRO_FINANCE_VOLUME_PATH)