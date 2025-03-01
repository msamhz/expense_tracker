from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
import time

@task
def extract_data(file_name: str):
    """Simulates extracting data with a delay."""
    logger = get_run_logger()
    time.sleep(2)  # Simulate delay
    logger.info(f"Extracted data from {file_name}")
    return f"data_from_{file_name}"

@task
def transform_data(extracted_data: str):
    """Simulates transforming data."""
    logger = get_run_logger()
    time.sleep(3)  # Simulate delay
    transformed_data = extracted_data.upper()
    logger.info(f"Transformed data: {transformed_data}")
    return transformed_data

@task
def load_data(transformed_data: str):
    """Simulates loading data."""
    logger = get_run_logger()
    time.sleep(1)  # Simulate delay
    logger.info(f"Loaded data: {transformed_data}")
    return "Load Complete"

@flow(name="ETL Flow Without Async", task_runner=ConcurrentTaskRunner())
def etl_pipeline():
    """Main ETL flow that runs using state and .submit()."""
    file_names = ["file1.csv", "file2.csv"]

    # Step 1: Extract in parallel
    extract_futures = [extract_data.submit(file) for file in file_names]

    # Step 2: Transform in parallel, after extraction completes
    transform_futures = [transform_data.submit(future.result()) for future in extract_futures]

    # Step 3: Load in parallel, after transformation completes
    load_futures = [load_data.submit(future.result()) for future in transform_futures]

    # Ensure all load tasks complete before exiting
    for task in load_futures:
        task.result()

if __name__ == "__main__":
    etl_pipeline()
