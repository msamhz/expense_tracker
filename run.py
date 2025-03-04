import subprocess
from prefect import task, flow
import time
from src.pipelines.etl import financial_etl_flow
from src import config  # Load the config
from src.ui.dashboard import run_dashboard  # Load the dashboard

@flow(name="ETL and Dashboard Flow")
def etl_dashboard_pipeline():
    """
    Orchestrates the ETL process followed by launching the dashboard.
    """
    financial_etl_flow(config)  # Pass config to the ETL flow
    run_dashboard()

# Run the full pipeline
if __name__ == "__main__":
    etl_dashboard_pipeline()