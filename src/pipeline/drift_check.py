from dotenv import load_dotenv
from evidently import Dataset, DataDefinition, Report
from evidently.presets import DataDriftPreset, DataSummaryPreset
import os
import pandas as pd
from prefect import flow, task, get_run_logger
from prefect.deployments import run_deployment
from sqlalchemy import create_engine
from sqlalchemy.orm import Session


load_dotenv()
logger = get_run_logger()
engine = create_engine(f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}/{os.getenv('POSTGRES_DB')}")


@task(name="read_data_from_db", retries=3, retry_delay_seconds=10)
def read_data_from_db():
    try:
        logger.info("Fetching the last 24 rows of sensor data from the Database...")
        with Session(engine) as session:
            result = session.execute(f"""
                SELECT * FROM {os.getenv('TABLE_NAME')}
                WHERE timestamp >= NOW() - INTERVAL '24 HOURS'
                ORDER BY timestamp DESC
                LIMIT 24;
            """)
            data = result.fetchall()
        logger.info("Data fetched successfully from the database.")
        return data
    except Exception as e:
        logger.error(f"Error fetching data from the database: {e}")
        raise

@task(name="converting_data_to_pandas", retries=3, retry_delay_seconds=10)
def converting_data_to_pandas(data):
    try:
        logger.info("Converting fetched data to Dataframe...")
        pd_data = pd.DataFrame(data)
        logger.info("Data converted to Dataframe successfully.")
        return pd_data
    except Exception as e:
        logger.error(f"Error converting data to Dataframe: {e}")
        raise

@task(name="check_for_drift")
def drift_check(data):
    try:
        logger.info("Checking for drift in data...")
        
        schema = DataDefinition(
            numerical_features=["SO2", "NO2", "CO", "O3", "TEMP", "PRES", "DEWP", "WSPM"],
            categorical_features=["wd"]
        )
        
        eval_data = Dataset.from_pandas(
            data, data_definition=schema
        )

        report = Report([
            DataDriftPreset(),
            DataSummaryPreset()
        ])

        my_eval = report.run(reference_data=None, current_data=eval_data)
        report.save_html("drift_report.html")
        logger.info("Drift check completed successfully. Report saved as 'drift_report.html'.")

    except Exception as e:
        logger.error(f"Couldn't check for drift due to: {e}")

@task(name="check_model_performance")
def model_performance_check():
    logger.info("Checking model performance...")
    # (Your code to check model performance using MLflow metrics)
    logger.info("Model performance check completed successfully.")

@flow(name="Flow-2-Daily-Drift")
def daily_drift():
    print("Fetching the last 24 rows of sensor data from the Database...")
    # (Your code to fetch data from your DB container)
    
    print("Checking for data drift using Evidently UI...")
    drift_detected = True  # Let's assume Evidently found drift
    
    if drift_detected:
        print("Alert! Drift found. Triggering Flow 3...")
        # This tells the server to immediately put Flow 3 into the work pool
        run_deployment(name="Flow-3-MLflow-Retrain/automated-retrain", timeout=0)

@flow(name="Flow-3-MLflow-Retrain")
def mlflow_retrain():
    print("Connecting to MLflow container...")
    print("Looping through 100 model combinations...")
    # (Your heavy loop to train models, track parameters, and pick the best one)

if __name__ == "__main__":
    # This is what Service 3 executes to register BOTH flows automatically
    daily_drift.deploy(
        name="automated-drift-check",
        work_pool_name="my-process-pool",
        cron="0 0 * * *"  # Run automatically every 24 hours
    )
    mlflow_retrain.deploy(
        name="automated-retrain",
        work_pool_name="my-process-pool"  # No cron! This only runs when Flow 2 calls it
    )