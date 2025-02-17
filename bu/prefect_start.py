import subprocess
from prefect import flow, task

@flow
def start_page():
    """Starts the main run.py script using subprocess."""
    subprocess.run(["streamlit", "run", "run.py", "--server.address",  "localhost"])  # ✅ Runs Streamlit locally

@flow(name="Start Page")
def prefect_run_flow():
    start_page()

if __name__ == "__main__":
    prefect_run_flow()
