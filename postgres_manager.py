import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os 
import pandas as pd
from psycopg2.extras import execute_values
from prefect import flow, task, get_run_logger


# Load environment variables from .env file
load_dotenv()

# Use environment variables for database configuration
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "FinanceTracker"),  # Default: FinanceTracker
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432")
}

@task
def create_table():
    """Creates the transactions table in PostgreSQL if it does not exist."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL PRIMARY KEY,
            transaction_date DATE NOT NULL,
            description TEXT NOT NULL,
            amount NUMERIC(12,2) NOT NULL,
            category TEXT,
            subcategory TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT unique_transaction UNIQUE (transaction_date, description, amount)  -- Ensures uniqueness
        );

        """

        cursor.execute(create_table_query)
        conn.commit()

        print("Table 'transactions' checked/created successfully.")

        cursor.close()
        conn.close()

    except Exception as e:
        print("Error:", e)

@task(name="Load Postgres")
def insert_transactions(df: pd.DataFrame):
    """Inserts multiple transactions into PostgreSQL while preventing duplicates."""
    if df.empty:
        print("⚠️ No data to insert.")
        return

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # insert_query = """
        # INSERT INTO transactions (transaction_date, description, amount, category, subcategory, processed_at)
        # VALUES %s
        # ON CONFLICT (transaction_date, description, amount) DO NOTHING;  -- Skip duplicates
        # """

        insert_query = """
        INSERT INTO transactions (transaction_date, description, amount, category, subcategory, processed_at)
        VALUES %s
        ON CONFLICT (transaction_date, description, amount) DO NOTHING;  -- Skip duplicates
        """
        
        data_tuples = [
            (
                row["transaction_date"],
                row["description"],
                row["amount"],
                row["category"] if isinstance(row["category"], str) else "Uncategorized",
                row["subcategory"] if isinstance(row["subcategory"], str) else "Uncategorized",
                row["processed_at"] if "processed_at" in df.columns else pd.Timestamp.now()
            )
            for _, row in df.iterrows()
        ]

        execute_values(cursor, insert_query, data_tuples)  # Bulk insert

        conn.commit()
        print(f"✅ Inserted {len(df)} new transactions (skipped duplicates).")

        cursor.close()
        conn.close()

    except Exception as e:
        print("Error:", e)

@flow(name="Load Postgres Flow")
def load_postgres_flow(df: pd.DataFrame):
    """Orchestrates the process of ensuring the table exists and inserting transactions."""
    create_table()  # Ensures table exists before inserting data
    insert_transactions(df)  # Inserts data into PostgreSQL


