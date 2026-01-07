import yaml
import os

def load_sources_config(config_path: str) -> dict:
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Missing config file: {config_path}")
    
    with open(config_path, "r") as f:
        return yaml.safe_load(f)