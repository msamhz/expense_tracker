import re
import os
import pdfplumber
import pandas as pd
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any

class TransactionParser:
    """A class to parse transaction data from bank statements in PDF format."""
    
    def __init__(self, input_dir: str = "data/raw", output_dir: str = "data/processed"):
        """
        Initialize the TransactionParser.
        
        Args:
            input_dir (str): Directory containing input PDF files
            output_dir (str): Directory where parsed CSV files will be saved
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        
    @staticmethod
    def parse_amount(text: str) -> Optional[float]:
        """
        Parse an amount string like 'S$248.80' or 'S$857.62 cr'.
        Returns a signed float (credits become negative).
        
        Args:
            text (str): The text containing the amount
            
        Returns:
            float or None: The parsed amount as a float, negative for credits
        """
        # Remove any commas and NBSPs
        t = text.replace(",", "").replace("\xa0", " ").strip()
        # Detect 'cr' for credit (negative)
        is_credit = t.lower().endswith(" cr")
        # Grab the numeric part after 'S$'
        m = re.search(r"S\$\s*([0-9]+(?:\.[0-9]{1,2})?)", t)
        if not m:
            return None
        amt = float(m.group(1))
        if is_credit:
            amt = -amt
        return amt

    @staticmethod
    def is_date_line(line: str) -> bool:
        """
        Check if a line starts with a date.
        
        Args:
            line (str): The line to check
            
        Returns:
            bool: True if the line starts with a date
        """
        return re.match(r"^\s*\d{1,2}\s+[A-Za-z]{3}\s+\d{4}\b", line) is not None

    @staticmethod
    def extract_date_desc_amt(line: str) -> Optional[Tuple[str, str, float]]:
        """
        Extract date, description and amount from a transaction line.
        
        Args:
            line (str): The line containing transaction data
            
        Returns:
            tuple or None: (date_iso, description, amount) if successful, None otherwise
        """
        # Split into 3 parts: date, description, amount
        # Amount is the last 'S$...' occurrence
        m_amount = list(re.finditer(r"S\$\s*[0-9]+(?:\.[0-9]{1,2})?(?:\s*cr)?\s*$", line, flags=re.IGNORECASE))
        if not m_amount:
            return None
        m = m_amount[-1]
        amount_str = line[m.start():m.end()]
        left = line[:m.start()].rstrip()

        # Date at start
        m_date = re.match(r"^\s*(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})\b", left)
        if not m_date:
            return None
        date_str = m_date.group(1)
        desc = left[m_date.end():].strip()
        # Clean odd characters (like replacement chars from PDF)
        desc = desc.replace("\xa0", " ").replace("�", "")

        # Parse date to ISO
        try:
            date_iso = datetime.strptime(date_str, "%d %b %Y").date().isoformat()
        except Exception:
            date_iso = date_str  # fallback to original

        amount = TransactionParser.parse_amount(amount_str)
        if amount is None:
            return None
            
        return date_iso, desc, amount

    def parse_pdf(self, pdf_path: str, save_csv: bool = True) -> pd.DataFrame:
        """
        Parse a PDF file containing bank transactions.
        
        Args:
            pdf_path (str): Path to the PDF file
            save_csv (bool): Whether to save the results as CSV
            
        Returns:
            pd.DataFrame: DataFrame containing the parsed transactions
        """
        rows: List[Dict[str, Any]] = []
        
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                # Normalize whitespace for consistent parsing
                lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

                # Skip entire sections we don't want:
                # - lines containing 'DBS Points' table
                # - 'PAYMENT - DBS INTERNET/WIRELESS' and its 'Sub-Total' row
                skip_payment_section = False
                for line in lines:
                    # Detect start of the "Payment - DBS Internet/Wireless" block
                    if "PAYMENT - DBS INTERNET/WIRELESS" in line.upper():
                        skip_payment_section = True
                        continue
                    if skip_payment_section:
                        # End skipping when we hit a 'Sub-Total' line or a blank separator or a new header
                        if line.upper().startswith("SUB-TOTAL") or line.upper().startswith("TRANSACTION DATE") or "DBS ALTITUDE" in line.upper():
                            skip_payment_section = False
                        continue  # while skipping, ignore

                    # Only consider lines that look like actual transaction rows
                    if self.is_date_line(line) and ("S$" in line):
                        out = self.extract_date_desc_amt(line)
                        if out is not None:
                            date_iso, desc, amount = out
                            # Basic guard: ignore if description looks like other headers or tables
                            if desc.upper().startswith(("DBS ALTITUDE", "TRANSACTION DATE", "SUB-TOTAL")):
                                continue
                            rows.append({
                                "transaction_date": date_iso,
                                "description": desc,
                                "amount_sgd": amount
                            })

        # Build DataFrame
        df = pd.DataFrame(rows, columns=["transaction_date", "description", "amount_sgd"])
        df = df.sort_values(by="transaction_date")
        
        if save_csv:
            # Create output directory if it doesn't exist
            os.makedirs(self.output_dir, exist_ok=True)
            
            # Generate output filename
            base_name = os.path.basename(pdf_path).split('.')[0]
            csv_path = os.path.join(self.output_dir, f"{base_name}_parsed.csv")
            
            # Save to CSV
            df.to_csv(csv_path, index=False)
            
        return df

    def process_file(self, filename: str, save_csv: bool = True) -> pd.DataFrame:
        """
        Process a PDF file from the input directory.
        
        Args:
            filename (str): Name of the PDF file in the input directory
            save_csv (bool): Whether to save the results as CSV
            
        Returns:
            pd.DataFrame: DataFrame containing the parsed transactions
        """
        pdf_path = os.path.join(self.input_dir, filename)
        return self.parse_pdf(pdf_path, save_csv=save_csv)
