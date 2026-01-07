import sys, os
from upload_utils import upload_to_databricks

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.paths import SOURCES_CONFIG_PATH
from utils.config_loader import load_sources_config

config = load_sources_config(SOURCES_CONFIG_PATH)["databricks"]

FILE_NAME = "historical_data.json"
HISTORICAL_FOREX_VOLUME_PATH = f"{config["path_volume"]["historical"]}{FILE_NAME}"

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

HISTORICAL_FOREX_LOCAL_PATH = f"historical_forex/{FILE_NAME}"

if __name__ == "__main__":
    upload_to_databricks(local_path=HISTORICAL_FOREX_LOCAL_PATH, volume_path=HISTORICAL_FOREX_VOLUME_PATH)
