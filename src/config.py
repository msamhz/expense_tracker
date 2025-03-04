import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# Directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Base directory of src
DATA_DIR = os.path.join(BASE_DIR, "..", "data")
PARAMS_FILE = os.path.join(BASE_DIR, "params.yaml")

RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed")
ERROR_DIR = os.path.join(DATA_DIR, "error")

# Ensure directories exist
for directory in [RAW_DATA_DIR, PROCESSED_DATA_DIR, ERROR_DIR]:
    os.makedirs(directory, exist_ok=True)

# Database credentials
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = os.getenv("DB_PORT", 5432)  # Default PostgreSQL port

# Prefect concurrency
TASK_RUNNER = "ConcurrentTaskRunner"

# Supported file types
FILE_EXTENSIONS = ["*.csv", "*.xls", "*.xlsx"]

# Database configuration using environment variables
DB_CONFIG = {
    "dbname": DB_NAME,  # Default: FinanceTracker
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST,
    "port": DB_PORT
}

# Export config as a dictionary
config = {
    "RAW_DATA_DIR": RAW_DATA_DIR,
    "PROCESSED_DATA_DIR": PROCESSED_DATA_DIR,
    "ERROR_DIR": ERROR_DIR,
    "DB_CONFIG": DB_CONFIG,
    "TASK_RUNNER": TASK_RUNNER,
    "FILE_EXTENSIONS": FILE_EXTENSIONS,
    'PARAMS_FILE': PARAMS_FILE
}