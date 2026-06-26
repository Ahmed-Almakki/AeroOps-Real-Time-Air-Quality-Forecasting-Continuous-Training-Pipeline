import os
import logging

import pandas as pd
from dotenv import load_dotenv

from src.input_data.kafka_input import KafkaInput

from ..data_prep.process_input import process_input

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    colorize=True,
)


def processed_data(input_data: pd.DataFrame) -> pd.DataFrame:
    try:
        logging.info("Starting to process input data.")
        processed_ready_data = process_input(input_data)
        return processed_ready_data
    except Exception as e:
        logging.error("Error occurred while processing input data: %s", e)
        return None


def inference(input_data: pd.DataFrame):
    try:
        logging.info("Starting the inference flow.")
        print("NOW YOU ARE IN PREDCITION")
        logging.info("Inference flow completed successfully.")
        return 0  # Replace with actual prediction
    except Exception as e:
        logging.error("Error occurred in the inference flow: %s", e)
        return None


if __name__ == "__main__":
    try:
        logging.info("Starting to ingest input data from Kafka topic.")
        kafka_input = KafkaInput(topic=os.getenv("KAFKA_TOPIC"))

        while True:
            msg = kafka_input.get_single_message()

            if msg is not None:
                logging.info("New row received from Kafka topic...")
                print('message received')
                data = processed_data(msg)
                prediction_result = inference(data)
            elif msg is None:
                logging.info("No new messages in Kafka topic. Waiting for new data...")
                continue
            else:
                logging.error("Data isn't ready to be processed: %s", data)

    except KeyboardInterrupt:
        logging.info("Script stopped manually by user.")

    except Exception as e:
        logging.error("Fatal error occurred: %s", e)

    finally:
        kafka_input.close()
