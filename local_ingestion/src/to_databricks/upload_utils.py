import sys, os
from databricks.sdk import WorkspaceClient
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.paths import SOURCES_CONFIG_PATH
from utils.config_loader import load_sources_config

logger = logging.getLogger(__name__)


def upload_to_databricks(local_path: str, volume_path: str):
    config = load_sources_config(SOURCES_CONFIG_PATH)["databricks"]

    PROFILE_NAME = config["credentials"]["profile_name"]

    w = WorkspaceClient(profile=PROFILE_NAME)

    try:
        with open(local_path, "rb") as f:
            w.files.upload(volume_path, f, overwrite=True)
            logger.info(f"Local file at {local_path} successfully uploaded to {volume_path}")

    except FileNotFoundError:
        logger.error(f"File not found at {local_path}")
        raise

    except Exception as e:
        logger.error(f"Error during the upload of the local file at {local_path}: {e}")
        raise
