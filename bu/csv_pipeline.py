import pandas as pd
import os
from prefect import flow, task, get_run_logger
import re
from postgres_manager import load_postgres_flow
from llm_inference import llm_inference
import asyncio

# Define file paths
RAW_DATA_DIR = "data/raw"
PROCESSED_DATA_DIR = "data/processed"

# Ensure directories exist
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

# Regex pattern to detect transaction lines (assuming YYYY-MM-DD or DD/MM/YYYY format)
transaction_pattern = re.compile(r"\b\d{2}/\d{2}/\d{4}\b")

@task
def detect_new_files():
    """Detects new CSV files in the raw data folder."""
    files = [f for f in os.listdir(RAW_DATA_DIR) if f.endswith(".csv")]
    
    if not files:
        print("No new CSV files detected.")
        return []

    print(f"Detected {len(files)} new files: {files}")
    return files

@task
def extract_transaction_lines(file_name: str) -> str:
    logger = get_run_logger()
    """Reads the file and extracts only transaction-related lines based on date patterns."""
    file_path = os.path.join(RAW_DATA_DIR, file_name)

    with open(file_path, "r", encoding="utf-8") as file:
        raw_content = file.readlines()

    # Return the first transaction line as the file name
    file_name_extracted = raw_content[0].split(",")[0]
    
    # Extract only relevant transaction lines
    transaction_lines = [line.strip() for line in raw_content if transaction_pattern.search(line)]

    if not transaction_lines:
        logger.info(f"No transaction data found in {file_name}.")
    else:
        logger.info(f"Extracted {len(transaction_lines)} transaction lines from {file_name}.")
    
    return file_name, file_name_extracted, transaction_lines

@task
def sc_preprocess(transaction_lines: list, filename: str):
    """Processes transaction lines, handling missing columns and merging non-SGD transactions into SGD Amount."""
    
    transaction_data = []

    for line in transaction_lines:
        parts = line.split(",")  # Split by commas

        # Handle cases where some lines have 4 columns, and others have 5
        if len(parts) >= 4:
            date = parts[0].replace("\t", "").strip() if len(parts) > 0 else None
            description = parts[1].strip() if len(parts) > 1 else None
            foreign_currency = parts[2].strip() if len(parts) > 2 and parts[2].strip() else None
            sgd_amount = parts[3].strip().replace("SGD", "").replace("DR", "").strip() if len(parts) > 3 else None
            extra_column = parts[4].strip().replace("SGD", "").replace("DR", "").strip() if len(parts) > 4 else None  # 5th column if available

            # Append to list
            transaction_data.append([date, description, foreign_currency, sgd_amount, extra_column])

    # Create DataFrame
    df_transactions = pd.DataFrame(transaction_data, columns=["Date", "Description", "Foreign Currency Amount", "SGD Amount", "Extra Column"])

    # Convert "SGD Amount" and "Extra Column" to numeric
    df_transactions["SGD Amount"] = pd.to_numeric(df_transactions["SGD Amount"], errors="coerce")
    df_transactions["Extra Column"] = pd.to_numeric(df_transactions["Extra Column"], errors="coerce")

    # Merge non-SGD transactions into the "SGD Amount" column (if available)
    df_transactions.loc[df_transactions["Foreign Currency Amount"].notna(), "SGD Amount"] = df_transactions["Extra Column"].fillna(df_transactions["SGD Amount"]).astype(float)

    # Drop the extra column after merging
    df_transactions.drop(columns=["Extra Column", "Foreign Currency Amount"], inplace=True)

    # Drop rows with missing SGD Amount
    df_transactions = df_transactions[~df_transactions["SGD Amount"].isnull()]

    # Rename columns to match the standard format
    df_transactions.rename(columns={"Date": "transaction_date", "Description": "description", "SGD Amount": "amount"}, inplace=True)
    
    # Clean and normalize date column before conversion
    df_transactions["transaction_date"] = df_transactions["transaction_date"].astype(str).str.strip()
    
    df_transactions["transaction_date"] = (df_transactions["transaction_date"]
                                .astype(str)  # Ensure it's a string
                                .str.replace(r"[^\d/]", "", regex=True)  # Remove non-date characters
                                .str.strip()  # Strip spaces
                                )

    # Attempt to convert, handling errors gracefully
    df_transactions["transaction_date"] = pd.to_datetime(
        df_transactions["transaction_date"], format="%d/%m/%Y", errors="coerce"
    )

    # Drop rows where conversion failed
    df_transactions = df_transactions.dropna(subset=["transaction_date"])

    # Format correctly for PostgreSQL
    df_transactions["transaction_date"] = df_transactions["transaction_date"].dt.strftime("%Y-%m-%d")


    return df_transactions



@task
def process_file(file, transaction_lines, file_name):
    """
    Preprocesses the file based on the first transaction line.
    
    Assuming first line contains the filename.
    """
    
    # Get file path
    file_path = os.path.join(RAW_DATA_DIR, file)
    logger = get_run_logger()
    
    # Get actual file name from first transaction line
    logger.info(f"Detected file: {file_name}...")
    
    if "SIMPLY CASH CREDIT CARD" in file_name:
        logger.info("Processing Standard Chartered transactions...")
        df = sc_preprocess(transaction_lines, file_name)

    df = llm_inference(df)

    # Add processed_at timestamp
    df['processed_at'] = pd.Timestamp.now().floor("S")

    processed_file_path = os.path.join(PROCESSED_DATA_DIR, f"{file_name}.csv")
    df.to_csv(processed_file_path, index=False)
    # os.remove(file_path)  # Delete raw file after processing

    logger.info(f"Processed file saved to {processed_file_path}.")
    
    return df

@task(name="Data Ingestion Flow")
def csv_pipeline():
    """Main flow that detects and processes CSV files."""
    
    # Detect new files
    files = detect_new_files()
    for file in files:
        oldfilename, file_name, df = extract_transaction_lines(file)
        df = process_file(oldfilename, df, file_name)
        load_postgres_flow(df)
        
if __name__ == "__main__":
    csv_pipeline()