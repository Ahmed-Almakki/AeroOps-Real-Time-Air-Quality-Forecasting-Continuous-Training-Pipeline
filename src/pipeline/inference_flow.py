import pandas as pd
from ..input_data.kafka_input import KafkaInput
from ..data_prep.process_input import process_input
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()



def processed_data(data: pd.DataFrame) -> pd.DataFrame:
    try:
        logging.info("Starting to process input data.")
        print(f"Received data for processing: {data}")
        data = process_input(data)
        logging.info("Input data processed successfully.")
        return data
    except Exception as e:
        logging.error(f"Error occurred while processing input data: {e}")
        # always raise error to trigger retry mechanism
        raise



try:
    logging.info("Starting to ingest input data from Kafka topic.")
    kafka_input = KafkaInput(topic=os.getenv("KAFKA_TOPIC"))
    msg = kafka_input.consume()

    if msg and not None:
        data = processed_data(msg)
        logging.info("Data ingested successfully from Kafka topic.")

        prediction_result = infernce(data)
    else:
        logging.error(f"Data isn't ready to be processed: {data}")

except Exception as e:
    logging.error(f"Error occurred while ingesting input data: {e}")



def infernce(data: pd.DataFrame) -> int:
    try:
        logging.info("Starting the inference flow.")
        print(f"NOW YOU ARE IN PREDCITION")
        logging.info("Inference flow completed successfully.")
    except Exception as e:
        logging.error(f"Error occurred in the inference flow: {e}")
        # always raise error to trigger retry mechanism
        raise


# @task(description="Predict the output", tags="Prediction", retry_delay_seconds=10, max_retries=5)
# def predict(input: pd.DataFrame) -> int:
#     try:
#         logging = get_run_logging()
#         logging.info("Starting prediction.")
#         # Add your prediction logic here
#         logging.info("Prediction completed successfully.")
#         return 0  # Replace with actual prediction
#     except Exception as e:
#         logging.error(f"Error occurred while making prediction: {e}")
#         # always raise error to trigger retry mechanism
#         raise


