import os
import json
import urllib.request as req

import pandas as pd
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
from evidently import Report, Dataset, Regression, DataDefinition
from sqlalchemy import text, create_engine
from sqlalchemy.orm import Session
from evidently.presets import DataDriftPreset, RegressionPreset
from prefect.deployments import run_deployment

load_dotenv()


def send_request(message: str) -> bool:
    logger = get_run_logger()
    slack_webhook = os.getenv('SLACK_WEBHOOK_URL', 'N/A')
    if slack_webhook == 'N/A':
        logger.error("Couldn't find the URL: url is %s", slack_webhook)
        return False
    try:
        logger.info("Start the process of sending an alert...")
        data = json.dumps({"text": message}).encode('utf-8')
        request = req.Request(
            slack_webhook, data=data, headers={"Content-Type": "application/json"}
        )
        with req.urlopen(request) as response:
            if response.status == 200:
                logger.info("Successfully send an alert to slack")
                return True
            logger.error("The response wasn't successfully returned %s", response)
            return False
    except Exception as e:
        logger.error("Failed to send request due to: %s", e)
        return False


@task(name="read_data_from_db", retries=3, retry_delay_seconds=10)
def read_data_from_db() -> tuple[pd.DataFrame, pd.DataFrame]:
    logger = get_run_logger()
    try:
        engine = create_engine(
            f"postgresql+psycopg://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}/{os.getenv('POSTGRES_DB')}"
        )
        logger.info("Fetching the last 24 rows of sensor data from the Database...")
        with Session(engine) as session:
            query = text(f"""
                SELECT * FROM {os.getenv('TABLE_NAME')}
                WHERE updated >= NOW() - INTERVAL '24 HOURS'
                ORDER BY updated DESC
                LIMIT 48;
            """)
            result = session.execute(query)

            data = result.fetchall()
            columns = result.keys()

            df = pd.DataFrame(data, columns=columns)
        logger.info("Fetched %s rows. and columns: %s", len(df), columns)

        df['updated'] = pd.to_datetime(df['updated'])
        # cutoff_time = df['updated'].max() - pd.Timedelta(minutes=1)
        # current_data = df[df['updated'] > cutoff_time].copy()
        # reference_data = df[df['updated'] <= cutoff_time].copy()
        current_data = df[:24].copy()
        reference_data = df[24:].copy()

        # Add this safety check!
        if len(current_data) < 24 or len(reference_data) < 24:
            logger.warning("Not enough data for drift check! Found %s total rows.", len(df))
            raise ValueError("Insufficient data to split into current and reference sets.")

        logger.info("Data fetched successfully from the database.")
        return current_data, reference_data
    except Exception as e:
        logger.error("Error fetching data from the database: %s", e)
        raise


@task(name="data_report", retries=3, retry_delay_seconds=10)
def data_report(current_data: pd.DataFrame, reference_data: pd.DataFrame) -> dict:
    try:
        logger = get_run_logger()
        logger.info("Checking for drift in data...")

        schema = DataDefinition(
            numerical_columns=["so2", "no2", "co", "o3", "temp", "pres", "dewp", "wspm"],
            categorical_columns=["wd"],
        )

        current_eval = Dataset.from_pandas(current_data, data_definition=schema)
        reference_eval = Dataset.from_pandas(reference_data, data_definition=schema)

        report = Report([DataDriftPreset()])

        my_eval = report.run(reference_data=reference_eval, current_data=current_eval)
        my_eval.save_html(
            f"/opt/prefect/reports/drift_report_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.html"
        )
        logger.info("Drift check completed successfully.")

        return my_eval.dict()
    except Exception as e:
        logger.error(f"Couldn't check for drift due to: {e}")
        raise


@task(name="check_for_data_drift", retries=3, retry_delay_seconds=10)
def check_for_drift(drift_report: dict) -> bool:
    logger = get_run_logger()
    logger.info("Starting checking for data drift...")
    threshold = 0.5
    try:
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
                        logger.warning(
                            "Drifted Column: %s | score: %s | threshold: %s method: %s",
                            col_name,
                            value,
                            col_threshold,
                            col_method,
                        )
            return True

        logger.info(
            "No significant data drift detected. drift_check_percentage: %s%%", drift_check * 100
        )
        return False
    except Exception as e:
        logger.error("Error checking for drift: %s", e)
        raise


@task(name="model_performance_report", retries=3, retry_delay_seconds=10)
def model_performance_report(current_data: pd.DataFrame, refrence_data: pd.DataFrame) -> dict:
    try:
        logger = get_run_logger()
        logger.info("Checking model performance...")
        schema = DataDefinition(
            numerical_columns=[
                "so2",
                "no2",
                "co",
                "o3",
                "temp",
                "pres",
                "dewp",
                "wspm",
                "prediction",
                "real_output",
            ],
            categorical_columns=["wd"],
            regression=[Regression(target="real_output", prediction="prediction")],
        )

        cur_data = Dataset.from_pandas(current_data, data_definition=schema)
        ref_data = Dataset.from_pandas(refrence_data, data_definition=schema)

        report = Report([RegressionPreset()], include_tests=True)
        model_performance_eval = report.run(current_data=cur_data, reference_data=ref_data)

        model_performance_eval.save_html(
            f"/opt/prefect/reports/model_performance_check_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.html"
        )
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
            result = 1 if item.get('status') == 'FAIL' else 0
            test_result.append((test_name, result))

        if sum(t[1] for t in test_result) == 0:
            logger.info("Model Performance Still Good")
            return False
        for name, _ in test_result:
            logger.warning("The test: %s Failed", name)
        logger.warning("Model Maybe Degrading, Calling Engineer...")
        return True
    except Exception as e:
        logger.error("Failed to check model performance due to: %s", e)
        raise


@flow(name="Flow-2-Daily-Drift")
def daily_drift():
    logger = get_run_logger()
    msg = ""
    trigger_training = False
    current_data, refrence_data = read_data_from_db()

    data_drift = data_report(current_data, refrence_data)
    is_drift = check_for_drift(data_drift)

    model_performance = model_performance_report(
        current_data=current_data, refrence_data=refrence_data
    )
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
        logger.info("Starting Re-training flow...")
        # deployment name usually <flow_name>/<deployment_nname> "deployment name as written in the .deploy(name=...)" flow name found in the @flow(name=)
        run_deployment(name="main_flow/automated-retrain-deployment", timeout=0)
        logger.info("Finish deployment successfully")


# @flow(name="Flow-3-MLflow-Retrain")
# def mlflow_retrain():
#     print("Connecting to MLflow container...")
#     print("Looping through 100 model combinations...")
#     # (Your heavy loop to train models, track parameters, and pick the best one)


if __name__ == "__main__":
    daily_drift.from_source(
        source="/opt/prefect/app", entrypoint="src/pipeline/drift_check.py:daily_drift"
    ).deploy(name="automated-drift-check", work_pool_name="my-process-pool", cron="*/1 * * * *")
