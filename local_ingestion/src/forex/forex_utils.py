import sys, os, json, time, requests, logging
from datetime import date, timedelta, datetime, timezone
from pydantic import BaseModel, ValidationError, AliasChoices, Field
from typing import Any

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.paths import SOURCES_CONFIG_PATH, SECRETS_DIR
from utils.config_loader import load_sources_config

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='app.log',
    filemode='a'
)
logger = logging.getLogger(__name__)

class ExchangeRateError(Exception): pass
class APIRequestFailed(ExchangeRateError): pass
class APITemporaryError(APIRequestFailed): pass
class DataValidationFailed(ExchangeRateError): pass

class ExchangeRates(BaseModel):
    date: str = Field(validation_alias=AliasChoices(
        'date',
        'updated'
    ))
    base: str
    results: dict[str, float]
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    class ConfigDict: extra = 'ignore'


config = load_sources_config(SOURCES_CONFIG_PATH)["forex"]

CREDENTIALS_FILE = os.path.join(SECRETS_DIR, config["credentials"]["credentials_file"])
API_KEY_NAME = config["credentials"]["api_key_name"]
API_BASE_URL_HISTORY = config["endpoints"]["historical"]
API_BASE_URL_LATEST = config["endpoints"]["latest"]
DAYS = config["days"]
BASE_CURRENCY = config["base_currency"]
MAX_RETRIES = config["retry"]["max_retries"]
RETRY_DELAY = config["retry"]["retry_delay"]

def get_fastforex_api_key() -> str:
    if not os.path.exists(CREDENTIALS_FILE):
        raise FileNotFoundError(f"Missing {CREDENTIALS_FILE}. Cannot load API key.")
    
    with open(CREDENTIALS_FILE, 'r') as f:
        config = json.load(f)

    return config.get(API_KEY_NAME)

def _fetch_data_from_api(endpoint_url: str) -> dict[str, Any]:
    headers = {"accept": "application/json"}

    try:
        response = requests.get(endpoint_url, headers=headers)

        status_code = response.status_code

        if status_code >= 400 and status_code < 500:
            raise APIRequestFailed(f"HTTP Permanent Error {status_code}") from None
        
        if status_code >= 500:
            raise APITemporaryError(f"HTTP Temporary Error {status_code}") from None
        
        response.raise_for_status()

        raw_data = response.json()       

        data = ExchangeRates(**raw_data)

        logger.info("Extraction and validation of data successful.")
        return data
    
    except requests.exceptions.Timeout as e:
        raise APITemporaryError(f"Temporary API Error (Timeout): {e}") from e
        
    except requests.exceptions.ConnectionError as e:
        raise APITemporaryError(f"Temporary API Error (Connection): {e}") from e
    
    except requests.exceptions.RequestException as e:
        raise APIRequestFailed(f"Unclassified Request Error: {e}") from e
        
    except (json.JSONDecodeError, ValidationError) as e:
        raise DataValidationFailed(f"JSON response is invalid or doesn't match the schema: {e}") from e


def _retry_on_temporary_error(api_call_function: callable, *args, **kwargs) -> dict[str, Any]:
    for attempt in range(MAX_RETRIES):
        try:
            data = api_call_function(*args, **kwargs)
            return data
        
        except (APIRequestFailed, DataValidationFailed) as e:
            logger.error(f"Permanent/Validation Error: {e}")
            raise

        except APITemporaryError as e:
            if attempt < MAX_RETRIES - 1:
                logger.warning(f"Temporary API Error: {e}. Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
                continue
            else:
                logger.error(f"Failed after {MAX_RETRIES} attempts. Last Temporary API Error: {e}")
                raise

def get_latest_exchange_rates() -> list[dict[str, Any]]:
    api_key = get_fastforex_api_key()
    endpoint_url = f"{API_BASE_URL_LATEST}from={BASE_CURRENCY}&api_key={api_key}"

    data = _retry_on_temporary_error(_fetch_data_from_api, endpoint_url)
    logger.info("Extraction and validation of latest data complete.")
    return [data]

def get_historical_exchange_rates() -> list[dict[str, Any]]:
    today = date.today()
    start_range = today - timedelta(days=DAYS)
    date_range: list[date] = [start_range + timedelta(days=i) for i in range((today-start_range).days)]
    all_historical_data: list[dict[str, Any]] = []

    api_key = get_fastforex_api_key()

    for d in date_range:
        date_str = d.strftime('%Y-%m-%d')

        time.sleep(1)

        endpoint_url = f"{API_BASE_URL_HISTORY}date={date_str}&from={BASE_CURRENCY}&api_key={api_key}"
        try:
            data = _retry_on_temporary_error(_fetch_data_from_api, endpoint_url)
            all_historical_data.append(data)

        except (APIRequestFailed, ValidationError) as e:
            logger.warning(f"Permanent/Validation Error for date {date_str}: {e}. Skipping this date.")
            continue

        except APITemporaryError as e:
            logger.error(f"Temporary API Error for date {date_str}: {e}.")
            raise

    logger.info(f"Historical extraction and validation complete. Total {len(all_historical_data)} days extracted.")
    return all_historical_data

