from forex_utils import get_latest_exchange_rates
from datetime import date
import os, json

def run_daily_extraction():
    output_dir = 'daily_forex'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    today = str(date.today())   
    daily_file = os.path.join(output_dir, f'{today}.json')
    daily_data = get_latest_exchange_rates()
    daily_data = [d.model_dump(mode='json') for d in daily_data]

    with open(file=daily_file, mode='w', encoding='utf-8') as f:
        json.dump(daily_data, f, indent=4)

    return

if __name__ == "__main__":
    run_daily_extraction()
