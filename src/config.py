import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# Directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Base directory of src
DATA_DIR = os.path.join(BASE_DIR, "..", "data")

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

# Export config as a dictionary
config = {
    "RAW_DATA_DIR": RAW_DATA_DIR,
    "PROCESSED_DATA_DIR": PROCESSED_DATA_DIR,
    "ERROR_DIR": ERROR_DIR,
    "DB_HOST": DB_HOST,
    "DB_USER": DB_USER,
    "DB_PASSWORD": DB_PASSWORD,
    "DB_NAME": DB_NAME,
    "DB_PORT": DB_PORT,
    "TASK_RUNNER": TASK_RUNNER,
    "FILE_EXTENSIONS": FILE_EXTENSIONS
}