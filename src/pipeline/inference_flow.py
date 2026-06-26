from dotenv import load_dotenv
import logging
import os
import pandas as pd
from src.input_data.kafka_input import KafkaInput
from ..data_prep.process_input import process_input


load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', colorize=True)



def processed_data(data: pd.DataFrame) -> pd.DataFrame:
    try:
        logging.info("Starting to process input data.")
        data = process_input(data)
        return data
    except Exception as e:
        logging.error(f"Error occurred while processing input data: {e}")
        return 


def inference(data: pd.DataFrame) -> int:
    try:
        logging.info("Starting the inference flow.")
        print(f"NOW YOU ARE IN PREDCITION")
        logging.info("Inference flow completed successfully.")
        return 0  # Replace with actual prediction
    except Exception as e:
        logging.error(f"Error occurred in the inference flow: {e}")


if __name__ == "__main__":
    try:
        logging.info("Starting to ingest input data from Kafka topic.")
        kafka_input = KafkaInput(topic=os.getenv("KAFKA_TOPIC"))

        while True:
            msg = kafka_input.get_single_message()

            if msg is not None:
                logging.info(f"New row received from Kafka topic...")
                print('message received')
                data = processed_data(msg)
                prediction_result = inference(data)
            elif msg is None:
                logging.info("No new messages in Kafka topic. Waiting for new data...")
                continue
            else:
                logging.error(f"Data isn't ready to be processed: {data}")

    except KeyboardInterrupt:
        logging.info("Script stopped manually by user.")

    except Exception as e:
        logging.error(f"Fatal error occurred: {e}")

    finally:
        kafka_input.close()
