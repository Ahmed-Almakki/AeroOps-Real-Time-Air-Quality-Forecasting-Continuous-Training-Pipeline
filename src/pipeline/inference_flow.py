import pandas as pd
import prefect
from prefect import flow, task, get_run_logger

@task(description="Taking input data to predict", tags="Processing_Input", retry_delay_seconds=10, max_retries=5)
def process_input(data: pd.DataFrame) -> pd.DataFrame:
    return

@task(description="Predict the output", tags="Prediction", retry_delay_seconds=10, max_retries=5)
def predict(input: pd.DataFrame) -> int:
    return

