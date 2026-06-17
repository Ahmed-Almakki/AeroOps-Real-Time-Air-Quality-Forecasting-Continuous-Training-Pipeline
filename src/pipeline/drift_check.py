from dotenv import load_dotenv
from evidently import Dataset, DataDefinition, Report, Regression
from evidently.presets import DataDriftPreset, RegressionPreset
import json
import os
import pandas as pd
from prefect import flow, task, get_run_logger
from prefect.deployments import run_deployment
from prefect.runner.storage import GitRepository
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
import urllib.request as req

load_dotenv()
engine = create_engine(f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}/{os.getenv('POSTGRES_DB')}")
repositry = GitRepository(url=os.getenv("GITHUB_REPO"), branch="dev", directories=["src"])


def send_request(message: str) -> bool:
    logger = get_run_logger()
    try:
        logger.info("Start the process of sending an alert...")
        slack_webhook = os.getenv('SLACK_WEBHOOK_URL', 'N/A')
        if slack_webhook == 'N/A':
            logger.error(f"Couldn't find the URL: url is {slack_webhook}")
        else:
            data = json.dumps({"text": message}).encode('utf-8')
            request = req.Request(slack_webhook, data=data, headers={"Content-Type": "application/json"})
            with req.urlopen(request) as response:
                logger.info("Successfully send an alert to slack")
                if response.status == 200:
                    return True
                return False
    except Exception as e:
        logger.error("Failed to send request due to: {e}")

@task(name="read_data_from_db", retries=3, retry_delay_seconds=10)
def read_data_from_db() -> tuple[pd.DataFrame, pd.DataFrame]:
    logger = get_run_logger()
    try:
        logger.info("Fetching the last 24 rows of sensor data from the Database...")
        with Session(engine) as session:
            query = text(f"""
                SELECT * FROM {os.getenv('TABLE_NAME')}
                WHERE updated >= NOW() - INTERVAL '48 SECONDS'
                ORDER BY timestamp DESC
                LIMIT 48;
            """)
            result = session.execute(query)

            data = result.fetchall()
            columns = result.keys()
            
            df = pd.DataFrame(data, columns=columns)
        logger.info(f"Fetched {len(df)} rows.")

        df['updated'] = pd.to_datetime(df['updated'])
        cutoff_time = df['updated'].max() - pd.Timedelta(seconds=24)
        current_data = df[df['updated'] > cutoff_time].copy()
        reference_data = df[df['updated'] <= cutoff_time].copy()

        logger.info("Data fetched successfully from the database.")
        return current_data, reference_data
    except Exception as e:

        logger.error(f"Error fetching data from the database: {e}")
        raise


@task(name="data_report", retries=3, retry_delay_seconds=10)
def data_report(current_data: pd.DataFrame, reference_data: pd.DataFrame) -> dict:
    try:
        logger = get_run_logger()
        logger.info("Checking for drift in data...")
        
        schema = DataDefinition(
            numerical_columns=["SO2", "NO2", "CO", "O3", "TEMP", "PRES", "DEWP", "WSPM"],
            categorical_columns=["wd"]
        )
        
        current_eval = Dataset.from_pandas(current_data, data_definition=schema)
        reference_eval = Dataset.from_pandas(reference_data, data_definition=schema)

        report = Report([
            DataDriftPreset()
        ])

        my_eval = report.run(reference_data=reference_eval, current_data=current_eval)
        my_eval.save_html(f"/opt/prefect/reports/drift_report_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.html")
        logger.info("Drift check completed successfully.")

        return my_eval.dict()
    except Exception as e:
        logger.error(f"Couldn't check for drift due to: {e}")
        raise


@task(name="check_for_data_drift", retries=3, retry_delay_seconds=10)
def check_for_drift(drift_report: dict) -> bool:
    try:
        logger = get_run_logger()
        logger.info("Starting checking for data drift...")
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
                    logger.warning(f"Drifted Column: {col_name} | score: {value} | threshold: {col_threshold} method: {col_method}")
            return True                
        
        logger.info(f"No significant data drift detected. drift_check_percentage: {drift_check * 100}%")
        return False
    except Exception as e:
        logger.error(f"Error checking for drift: {e}")
        raise


@task(name="model_performance_report", retries=3, retry_delay_seconds=10)
def model_performance_report(current_data: pd.DataFrame, refrence_data: pd.DataFrame) -> dict:
    try:
        logger = get_run_logger()
        logger.info("Checking model performance...")
        schema = DataDefinition(
            numerical_columns=["SO2", "NO2", "CO", "O3", "TEMP", "PRES", "DEWP", "WSPM"],
            categorical_columns=["wd"],
            regression=[Regression(target="PM2.5", prediction="prediction")]
        )

        cur_data = Dataset.from_pandas(current_data, data_definition=schema)
        ref_data = Dataset.from_pandas(refrence_data, data_definition=schema)

        report = Report([
            RegressionPreset()
        ], includ_test=True)
        model_performance_eval = report.run(current_data=cur_data, reference_data=ref_data)

        model_performance_eval.save_html(f"/opt/prefect/reports/model_performance_check_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.html")
        logger.info("Model performance check completed successfully.")

        return model_performance_eval.dict()
    except Exception as e:
        logger.error(f"performance check failed due to: {e}")
        raise

@task(name="model_performance_check", retries=3, retry_delay_seconds=10)
def model_performance_check(performance: dict) -> bool: 
    try:
        logger = get_run_logger()
        logger.info('Start checking model performance...')
        tests = performance['tests']
        test_result = []
        for item in tests:
            test_name = item['name']
            result = 1 if item['status'].value == 'FAIL' else 0
            test_result.append((test_name, result))

        if sum(t[1] for t in test_result) == 0:
            logger.info("Model Performance Still Good")
            return False
        for name, _ in test_result:
             logger.warning(f"The test: {name} Failed")
        logger.warning("Model Maybe Degrading, Calling Engineer...")
        return True
    except Exception as e:
        logger.error(f"Failed to check model performance due to: {e}")
        raise



@flow(name="Flow-2-Daily-Drift")
def daily_drift():
    logger = get_run_logger()
    msg = ""
    trigger_training = False
    current_data, refrence_data = read_data_from_db()

    data_drift = data_report(current_data, refrence_data)
    is_drift = check_for_drift(data_drift)
    
    model_performance = model_performance_report(current_data=current_data, refrence_data=refrence_data)
    is_degraded = model_performance_check(model_performance)
    
    if is_drift and not is_degraded:
        logger.warning("Data Drifted but model still performing good")
        msg = "A Data Drift is Suspected"

    elif is_degraded and not is_drift:
        msg = "Model performance degergation is suspected"
        trigger_training = True
        logger.error("Model degrading while data isn't drifting")
        logger.warning("Triggering Training FLow")


    elif is_drift and is_degraded:
        logger.warning("Check Sensors reads...")
        msg = "Both data drift and model degregation is suspected"

    else:
        logger.info("ALl good")
        return
    
    send_request(message=msg)
    if trigger_training:
        run_deployment(name="Flow-3-MLflow-Retrain/automated-retrain", timeout=0)


@flow(name="Flow-3-MLflow-Retrain")
def mlflow_retrain():
    print("Connecting to MLflow container...")
    print("Looping through 100 model combinations...")
    # (Your heavy loop to train models, track parameters, and pick the best one)



if __name__ == "__main__":
    daily_drift.from_source(
        source=repositry,
        entrypoint="src/pipeline.drift_check.py:daily_drift"
    ).deploy(
        name="automated-drift-check",
        work_pool_name="my-process-pool",
        cron="0 0 * * *"
    )
    
    mlflow_retrain.deploy(
        name="automated-retrain",
        work_pool_name="my-process-pool"  # No cron! This only runs when Flow 2 calls it
    )