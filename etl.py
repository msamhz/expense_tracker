import os
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from dotenv import load_dotenv
import glob
import re
import shutil 
import traceback
  
  
from postgres_manager import load_postgres_flow
from llm_inference import llm_inference

# Load environment variables from .env file
load_dotenv()

# Other configuration
RAW_DATA_DIR = "data/raw"
PROCESSED_DATA_DIR = "data/processed"
ERROR_DIR = "data/error"

# Create directories if they don't exist
for directory in [RAW_DATA_DIR, PROCESSED_DATA_DIR, ERROR_DIR]:
    os.makedirs(directory, exist_ok=True)

# Regex pattern to detect transaction lines (assuming YYYY-MM-DD or DD/MM/YYYY format)
transaction_pattern = re.compile(r"\b\d{2}/\d{2}/\d{4}\b")

@task
def extract_transaction_lines(file_name: str) -> str:
    logger = get_run_logger()
    """Reads the file and extracts only transaction-related lines based on date patterns."""

    with open(file_name, "r", encoding="utf-8") as file:
        raw_content = file.readlines()

    # Return the first transaction line as the file name
    file_name_extracted = raw_content[0].split(",")[0]
    
    # Extract only relevant transaction lines
    transaction_lines = [line.strip() for line in raw_content if transaction_pattern.search(line)]

    if not transaction_lines:
        logger.info(f"No transaction data found in {file_name}.")
    else:
        logger.info(f"Extracted {len(transaction_lines)} transaction lines from {file_name}.")
    
    return str(file_name_extracted), transaction_lines

@task(name="detect_bank_format")
def detect_bank_format(file_path: str) -> Tuple[str, str, Optional[List[str]]]:
    """Detect which bank format the CSV file is from based on its structure."""
    logger = get_run_logger()
    file_name, transaction_lines = extract_transaction_lines(file_path)
    
    try:
        if "SIMPLY CASH CREDIT CARD" in file_name:
            logger.info("Processing Standard Chartered transactions...")
            return "sc", file_name, transaction_lines
        elif "UOB Krisflyer CREDIT CARD" in file_name:
            return "kris", file_name, transaction_lines
        elif "DBS Altitude CREDIT CARD" in file_name:
            return "altitude", file_name, transaction_lines
        else:
            return "unknown", "unknown", None
    except Exception as e:
        logger.error(f"Error detecting bank format for {file_path}: {e}")
        return "unknown", "unknown", None

@task(name="parse_sc_transactions")
def parse_sc_transactions(filename: str, transactions: str) -> pd.DataFrame:
    """Parse Standard Charted format CSV files."""
    
    transaction_data = []

    for line in transactions:
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
    
    df_transactions['bank_account'] = 'Standard Chartered'
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_filename = f"{timestamp}_{filename}"
    df_transactions['source_file'] = new_filename
    
    return df_transactions


@task(name="parse_bank_b")
def parse_bank_b(file_path: str) -> pd.DataFrame:
    """Parse Bank B format CSV files."""
    df = pd.read_csv(file_path)
    
    # Standardize column names and formats
    result_df = pd.DataFrame({
        "transaction_date": pd.to_datetime(df["Date"]),
        "description": df["Description"],
        "amount": df["Credit"].fillna(0) - df["Debit"].fillna(0),
        "category": df.get("Category", "Uncategorized"),
        "bank_account": f"Bank B - {df.get('Account', 'Unknown')}",
        "source_file": os.path.basename(file_path)
    })
    
    return result_df

@task(name="parse_bank_c")
def parse_bank_c(file_path: str) -> pd.DataFrame:
    """Parse Bank C format CSV files."""
    df = pd.read_csv(file_path)
    
    # Standardize column names and formats
    result_df = pd.DataFrame({
        "transaction_date": pd.to_datetime(df["Posted Date"]),
        "description": df["Description"],
        "amount": df["Amount"],  # Assume amount is already positive/negative
        "category": df.get("Category", "Uncategorized"),
        "bank_account": f"Bank C - {df.get('Account ID', 'Unknown')}",
        "source_file": os.path.basename(file_path)
    })
    
    return result_df

@task(name="move_processed_file")
def move_processed_file(file_path: str, new_filename: str = None, success: bool = True) -> None:
    """Move processed file to success or error directory."""
    logger = get_run_logger()

    if new_filename is None:
        new_filename = os.path.basename(file_path)  # Use original filename if not provided

    destination_dir = PROCESSED_DATA_DIR if success else ERROR_DIR
    os.makedirs(destination_dir, exist_ok=True)  # Ensure the directory exists

    destination = os.path.join(destination_dir, new_filename + ".csv")

    try:
        shutil.move(file_path, destination)  # More robust than os.rename
        logger.info(f"Moved {file_path} to {destination}")
    except Exception as e:
        logger.error(f"Failed to move {file_path} to {destination}: {e}")
        raise
    
@task(name="parse_transactions")
def parse_transactions(filename: str, bank_format: str, transactions: str) -> pd.DataFrame:
    """Parse bank transactions based on detected format."""
    parser_functions = {
        "sc": parse_sc_transactions,
        "bank_b": parse_bank_b,
        "bank_c": parse_bank_c
    }
    return parser_functions[bank_format](filename, transactions)


@task(name="process_file", retries=2)
def process_file(file_path: str) -> Dict[str, Any]:
    """Process a single file through the entire ETL pipeline."""
    logger = get_run_logger()
    logger.info(f"Processing file: {file_path}")
    
    try:
        bank_format, filename, transactions = detect_bank_format(file_path)
        
        if bank_format == "unknown":
            logger.warning(f"Unknown bank format for {file_path}")
            move_processed_file(file_path, filename, success=False)
            return {"file": file_path, "status": "error", "reason": "Unknown bank format"}
        
        df = parse_transactions(filename, bank_format, transactions)
        df = llm_inference(df)

        load_postgres_flow(df)
        
        new_filename = df.get('source_file').tolist()[0]
        logger.info(f"f{new_filename} Here is the first source_file")
        move_processed_file(file_path, new_filename, success=True)
        
        return {
            "file": file_path,
            "status": "success",
            "bank_format": bank_format,
            "transactions": len(df),
            "dataframe": df
        }
 
    except Exception as e:
        error_message = traceback.format_exc()  # Get the full traceback
        logger.error(f"❌ Error processing {file_path}: {error_message}")
        move_processed_file(file_path, success=False)
        return {"file": file_path, "status": "error", "reason": str(e)}

@flow(name="Financial CSV ETL", task_runner=ConcurrentTaskRunner())
def financial_etl_flow() -> List[Dict[str, Any]]:
    """Main ETL flow that processes all CSV files in parallel."""
    logger = get_run_logger()
    # Find all CSV files in the input directory
    csv_files = glob.glob(os.path.join(RAW_DATA_DIR, "*.csv"))
    logger.info(f"Found {len(csv_files)} CSV files to process")
    
    if not csv_files:
        logger.info("No CSV files found in input directory")
        return []
    
    logger.info(f"Found {len(csv_files)} CSV files to process")
    
    # Process each file in parallel
    results = []
    for file_path in csv_files:
        # The map function allows these tasks to run in parallel
        result = process_file.submit(file_path)
        results.append(result)
    
    return results

if __name__ == "__main__":
    financial_etl_flow()
