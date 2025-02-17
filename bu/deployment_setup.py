from prefect_start import prefect_run_flow  # Import your Prefect flow

if __name__ == "__main__":
    prefect_run_flow.serve(
        name="Start-Page",
    )
