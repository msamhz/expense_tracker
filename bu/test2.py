import psycopg2
import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ✅ Ensure the script explicitly uses FinanceTracker
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "FinanceTracker"),  # Default: FinanceTracker
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432")
}

print(f"✅ Connecting to database: {DB_CONFIG['dbname']}")

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT current_database();")
    db_name = cursor.fetchone()[0]
    print(f"✅ Connected to database: {db_name}")
    cursor.close()
    conn.close()
except Exception as e:
    print(f"❌ Connection failed: {e}")
