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

def check_uob(df: pd.DataFrame) -> bool:
    """
    Checks if the given DataFrame corresponds to a UOB transaction file.

    Args:
        df (pd.DataFrame): The DataFrame loaded from an Excel or CSV file.

    Returns:
        bool: True if the DataFrame contains "United Overseas Bank Limited" in the first column, otherwise False.
    """
    try:
        return "United Overseas Bank Limited" in df.columns[0]
    except IndexError:
        return False

def check_sc(raw_content: list) -> bool:
    """
    Checks if the given list of strings corresponds to a Standard Chartered (SC) transaction file.

    Args:
        raw_content (list): A list of strings read from a text-based CSV or UTF-8 file.

    Returns:
        bool: True if the first transaction line contains "SIMPLY CASH CREDIT CARD", otherwise False.
    """
    try:
        file_name_extracted = raw_content[0].split(",")[0]
        return "SIMPLY CASH CREDIT CARD" in file_name_extracted
    except IndexError:
        return False

def detect_bank(artifact: str | pd.DataFrame) -> tuple[str, pd.DataFrame | list] | None:
    logger = get_run_logger()
    """
    Detects the bank type based on the file content. It uses a try-except approach to handle different formats.

    Args:
        artifact (str | pd.DataFrame): The processed file content, either as a list of strings (for text-based files) or a DataFrame (for Excel/CSV).

    Returns:
        tuple[str, pd.DataFrame | list] | None:
            - If a Standard Chartered (SC) file is detected, returns ("Simplified Cashback Card", list of transaction lines).
            - If a UOB file is detected, returns ("United Overseas Bank Limited", DataFrame).
            - If no match is found, returns None.
    """
    try:
        # Try detecting Standard Chartered transactions
        if check_sc(artifact):
            logger.info("SC transaction detected")
            transaction_lines = extract_transaction_lines(artifact)
            return "Simplified Cashback Card", transaction_lines
    except Exception as e:
        pass

        try:
            # Try detecting United Overseas Bank transactions
            if check_uob(artifact):
                logger.info("UOB transaction detected")
                return "United Overseas Bank Limited", artifact
        except Exception as e:
            pass

    logger.info("Bank type could not be detected.")
    return None, None

def extract_transaction_lines(raw_content: list) -> list:
    """
    Extracts only transaction-related lines from a list of strings based on date patterns.

    Args:
        raw_content (list): List of strings representing the file content.

    Returns:
        list: Extracted transaction lines containing valid transaction dates.
    """
    transaction_pattern = re.compile(r"\b\d{2}/\d{2}/\d{4}\b")  # Matches dates in DD/MM/YYYY format

    transaction_lines = [line.strip() for line in raw_content if transaction_pattern.search(line)]
    return transaction_lines

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


@task(name="parse_uob_transactions")
def parse_uob_transactions(filename: str, df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    """Parse UOB credit card statements and format to match expected output without re-reading the file."""

    # Define expected column names
    expected_columns = [
        "Transaction Date", "Posting Date", "Description", 
        "Foreign Currency Type", "Transaction Amount(Foreign)", 
        "Local Currency Type", "Transaction Amount(Local)"
    ]

    # Escape special characters for regex matching
    escaped_columns = [re.escape(col) for col in expected_columns]

    # Find the correct header row
    header_row = None
    for i in range(15):  # Iterate through first 15 rows
        if df.iloc[i].astype(str).str.contains('|'.join(escaped_columns), case=False, regex=True, na=False).sum() > 4:
            header_row = i
            break

    if header_row is not None:
        logger.info(f"Header found at row {header_row}")
    else:
        logger.info("No suitable header row found within the first 15 rows.")
        return None  # Stop execution if no header row is found

    # Manually set header row **without re-reading the file**
    df.columns = df.iloc[header_row]  # Set new header row
    df = df.iloc[header_row + 2:].reset_index(drop=True)  # Drop the header row and fully reset the index

    # Rename columns to match expected format
    rename_mapping = {
        "Transaction Date": "transaction_date",
        "Description": "description",
        "Transaction Amount(Local)": "amount"
    }
    
    # Select relevant columns and rename
    df = df[list(rename_mapping.keys())].rename(columns=rename_mapping)

    # Ensure "amount" column is numeric
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    # Clean and normalize date column before conversion
    df["transaction_date"] = df["transaction_date"].astype(str).str.strip()
    
    # Convert date column to YYYY-MM-DD format
    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"], format="%d %b %Y", errors="coerce"
    )
    
    # Add bank identifier
    df["bank_account"] = "UOB"

    # Generate a unique source file name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_filename = f"{timestamp}_{filename}"
    df["source_file"] = new_filename

    return df

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

def read_file(file_path: str) -> str | pd.DataFrame | None:
    logger = get_run_logger()
    """
    Reads a given file and returns its content in an appropriate format.

    Attempt order:
    1. Reads as a UTF-8 text file and returns a list of strings.
    2. If UTF-8 fails, attempts to read as an Excel file (`.xls` or `.xlsx`) and returns a DataFrame.
    3. If Excel fails, attempts to read as a CSV and returns a DataFrame.
    4. If all attempts fail, returns None.

    Args:
        file_path (str): The path to the file.

    Returns:
        str | pd.DataFrame | None:
            - List of strings if read as a UTF-8 text file.
            - DataFrame if read as an Excel or CSV file.
            - None if all reading attempts fail.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            raw_content = file.readlines()
        logger.info("File read successfully as UTF-8 text.")
        return raw_content  # Returns a list of strings

    except UnicodeDecodeError:
        logger.info("UTF-8 decoding failed. Trying as an Excel file...")

        try:
            df = pd.read_excel(file_path, engine="xlrd")
            logger.info("File read successfully as Excel.")
            return df  # Returns a DataFrame

        except Exception as e:
            logger.info(f"Excel reading failed: {e}. Trying as a CSV...")

            try:
                df = pd.read_csv(file_path)
                logger.info("File read successfully as CSV.")
                return df  # Returns a DataFrame

            except Exception as e:
                logger.info(f"CSV reading failed: {e}. Unable to process file.")
                return None  # Return None if all methods fail
            
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
def parse_transactions(filename: str, transactions: str) -> pd.DataFrame:
    """Parse bank transactions based on detected format."""
    parser_functions = {
        "Simplified Cashback Card": parse_sc_transactions,
        "United Overseas Bank Limited": parse_uob_transactions,
        "bank_c": parse_bank_c
    }
    return parser_functions[filename](filename, transactions)


@task(name="process_file", retries=2)
def process_file(file_path: str) -> Dict[str, Any]:
    """Process a single file through the entire ETL pipeline."""
    logger = get_run_logger()
    logger.info(f"Processing file: {file_path}")
    
    try:
        # Read the file content
        artifact = read_file(file_path)
        
        card_name, artifact = detect_bank(artifact)
        
        if card_name == "unknown":
            logger.warning(f"Unknown bank format for {file_path}")
            move_processed_file(file_path, card_name, success=False)
            return {"file": file_path, "status": "error", "reason": "Unknown bank format"}
        
        df = parse_transactions(card_name, artifact)
        df = llm_inference(df)

        load_postgres_flow(df)
        
        new_filename = df.get('source_file').tolist()[0]
        logger.info(f"f{new_filename} Here is the first source_file")
        move_processed_file(file_path, new_filename, success=True)
        
        return {
            "file": file_path,
            "status": "success",
            "bank_format": card_name,
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
    """Main ETL flow that processes all financial files (CSV, Excel) in parallel, excluding .gitkeep."""
    logger = get_run_logger()
    
    # Supported file extensions
    file_extensions = ["*.csv", "*.xls", "*.xlsx"]

    # Find all matching files in the directory, excluding .gitkeep
    all_files = []
    for ext in file_extensions:
        all_files.extend(glob.glob(os.path.join(RAW_DATA_DIR, ext)))

    # Exclude .gitkeep explicitly
    all_files = [file for file in all_files if not file.endswith(".gitkeep")]

    logger.info(f"Found {len(all_files)} files to process: {all_files}")

    if not all_files:
        logger.info("No valid financial files found in input directory.")
        return []

    # Process each file in parallel
    results = []
    for file_path in all_files:
        result = process_file.submit(file_path)  # Submit each file for parallel processing
        results.append(result)

    return results

if __name__ == "__main__":
    financial_etl_flow()
