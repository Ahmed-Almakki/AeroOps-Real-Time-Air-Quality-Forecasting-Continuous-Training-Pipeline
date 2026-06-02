import pandas as pd
import prefect
from prefect import flow, task, get_run_logger
from ..input_data.kafka_input import KafkaInput
import os
from dotenv import load_dotenv
load_dotenv()

@task(description="Ingesting input data from Kafka topic", tags="Ingesting_Input", retry_delay_seconds=10, max_retries=5)
def ingest_input() -> pd.DataFrame:
    try:
        logger = get_run_logger()
        logger.info("Starting to ingest input data from Kafka topic.")
        kafka_input = KafkaInput(topic=os.getenv("KAFKA_TOPIC"))
        data = kafka_input.consume()
        logger.info("Data ingested successfully from Kafka topic.")
        return pd.DataFrame([data])
    except Exception as e:
        logger.error(f"Error occurred while ingesting input data: {e}")

@task(description="Taking input data to predict", tags="Processing_Input", retry_delay_seconds=10, max_retries=5)
def process_input(data: pd.DataFrame) -> pd.DataFrame:
    try:
        logger = get_run_logger()
        logger.info("Starting to process input data.")
        # Add your data processing logic here
        logger.info("Input data processed successfully.")
        return data
    except Exception as e:
        logger.error(f"Error occurred while processing input data: {e}")


@task(description="Predict the output", tags="Prediction", retry_delay_seconds=10, max_retries=5)
def predict(input: pd.DataFrame) -> int:
    try:
        logger = get_run_logger()
        logger.info("Starting prediction.")
        # Add your prediction logic here
        logger.info("Prediction completed successfully.")
        return 0  # Replace with actual prediction
    except Exception as e:
        logger.error(f"Error occurred while making prediction: {e}")


