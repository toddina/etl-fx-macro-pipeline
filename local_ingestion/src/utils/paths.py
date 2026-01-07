import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))

SRC_DIR = os.path.join(ROOT_DIR, "src")
CONFIG_DIR = os.path.join(ROOT_DIR, "config")
SECRETS_DIR = os.path.join(ROOT_DIR, "secrets")

SOURCES_CONFIG_PATH = os.path.join(CONFIG_DIR, "sources.yml")
