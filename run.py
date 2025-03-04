import subprocess
from prefect import task, flow
import time
from src.pipelines.etl import financial_etl_flow
from src import config  # Load the config

@task(name="Launch Dashboard")
def launch_dashboard():
    """
    Launches the Panel dashboard on localhost after ETL completion.
    """
    print("🎯 Launching Financial Dashboard on localhost...")

    # Run the Panel app in a subprocess
    subprocess.Popen(["python", "dashboard.py"])  # Replace with your actual script filename

    print("✅ Dashboard launched! Open http://localhost:5006")

@flow(name="ETL and Dashboard Flow")
def etl_dashboard_pipeline():
    """
    Orchestrates the ETL process followed by launching the dashboard.
    """
    financial_etl_flow(config)  # Pass config to the ETL flow
    launch_dashboard()

# Run the full pipeline
if __name__ == "__main__":
    etl_dashboard_pipeline()