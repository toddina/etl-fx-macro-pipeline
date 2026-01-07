from forex_utils import get_historical_exchange_rates
import os, json

def run_historical_extraction():
    output_dir = 'historical_forex'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    historical_file = os.path.join(output_dir, 'historical_data.json')
    historical_data = get_historical_exchange_rates()
    historical_data = [d.model_dump(mode='json') for d in historical_data]

    with open(file=historical_file, mode='w', encoding='utf-8') as f:
        json.dump(historical_data, f, indent=4)
    
    return

if __name__ == "__main__":
    run_historical_extraction()
