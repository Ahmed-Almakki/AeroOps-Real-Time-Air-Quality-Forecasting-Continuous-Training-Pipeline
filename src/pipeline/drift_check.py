from dotenv import load_dotenv
from evidently import Dataset, DataDefinition, Report
from evidently.presets import DataDriftPreset
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
                WHERE timestamp >= NOW() - INTERVAL '48 HOURS'
                ORDER BY timestamp DESC
                LIMIT 48;
            """)

            data = result.fetchall()
            columns = result.keys()
            
            df = pd.DataFrame(data, columns=columns)
            logger.info(f"Fetched {len(df)} rows of data from the database.")

            current_data = df.iloc[:24].copy()
            reference_data = df.iloc[24:].copy()

        logger.info("Data fetched successfully from the database.")
        return current_data, reference_data
    except Exception as e:
        logger.error(f"Error fetching data from the database: {e}")
        raise


@task(name="result_of_data_checks", retries=3, retry_delay_seconds=10)
def data_check(current_data: pd.DataFrame, reference_data: pd.DataFrame):
    try:
        logger.info("Checking for drift in data...")
        
        schema = DataDefinition(
            numerical_features=["SO2", "NO2", "CO", "O3", "TEMP", "PRES", "DEWP", "WSPM"],
            categorical_features=["wd"]
        )
        
        current_eval = Dataset.from_pandas(current_data, data_definition=schema)
        reference_eval = Dataset.from_pandas(reference_data, data_definition=schema)

        report = Report([
            DataDriftPreset()
        ])

        my_eval = report.run(reference_data=reference_eval, current_data=current_eval)
        my_eval.save_html(f"drift_report_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.html")
        logger.info("Drift check completed successfully.")

        return my_eval.dict()
    except Exception as e:
        logger.error(f"Couldn't check for drift due to: {e}")
        raise


@task(name="check_for_drift", retries=3, retry_delay_seconds=10)
def check_for_drift(drift_report: dict) -> bool:
    try:
        threshold = 0.5
        metrics = drift_report['metrics']

        drift_check = metrics[0]['value']['share']
        if drift_check >= threshold:
            logger.warning("Significant data drift detected!")

            for item in metrics[1:]:
                config = item.get('config', {})
                value = item.get('value')
    
                if config.get('type') == 'evidently:metric_v2:ValueDrift':
                    col_name = config.get('column', 'N/A')
                    col_threshold = config.get('threshold', 0.0)
                    col_method = config.get('method', 'N/A')
                
                if value < col_threshold:
                    logger.warning(f"Drifted Coulmn: {col_name}, threshold: {col_threshold} method: {col_method}")
            return True                
        
        logger.info(f"No significant data drift detected. drift_check_percentage: {drift_check * 100}%")
        return False
    except Exception as e:
        logger.error(f"Error checking for drift: {e}")
        raise

@task(name="check_model_performance")
def model_performance_check(drift: bool):
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