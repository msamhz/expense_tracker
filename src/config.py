import os
import configparser

# Load configuration
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "../config.ini"))

# Set default paths
BASE_PATH = config.get("Paths", "BASE_PATH", fallback=os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))
UPLOAD_FOLDER = os.path.join(BASE_PATH, config.get("Paths", "UPLOAD_FOLDER", fallback="data/raw"))
PROCESSED_FOLDER = os.path.join(BASE_PATH, config.get("Paths", "PROCESSED_FOLDER", fallback="data/processed"))
