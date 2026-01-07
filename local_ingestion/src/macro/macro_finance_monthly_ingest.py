import yfinance as yf
import logging
from datetime import date, datetime, timezone

logger = logging.getLogger(__name__)

def download_macro_finance():
    tickers = {
        "US_10Y_Yield": "^TNX",      
        "Gold": "GLD",              
        "Oil": "USO",               
        "EuroStoxx50": "^STOXX50E"  
    }      

    try:
        data = yf.download(list(tickers.values()), period="1mo", interval="1d")

        if not data.empty and 'Close' in data:
            df = data['Close'].copy()
            df['ingested_at'] = datetime.now(timezone.utc)

            today = str(date.today())
            monthly_file = f"monthly_macro_finance/{today}"

            df.to_parquet(f'{monthly_file}.parquet', coerce_timestamps="us")

            logger.info("Extraction of data successful")
            return df  

        else:
            logger.warning("The dataset is empty or column 'Close' missing.")

    except Exception as e:
        logger.error(f"Critical error during download: {e}")

if __name__ == '__main__':
    download_macro_finance()