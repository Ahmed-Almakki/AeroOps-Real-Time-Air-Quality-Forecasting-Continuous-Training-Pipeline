import os
import logging
import mlflow
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
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


def inference(input_data: pd.DataFrame, model):
    try:
        logging.info("Starting the inference flow.")

        result = model.predict(input_data)

        logging.info("Inference flow completed successfully with result: %s", result)

        return result
    except Exception as e:
        logging.error("Error occurred in the inference flow: %s", e)
        return None


def insert_data(reading_time, prediction_result):
    try:
        logging.info("Inserting prediction result into the database.")

        engine = create_engine(
            f"postgresql+psycopg://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB')}"
        )

        if isinstance(prediction_result, (list, np.ndarray)):
            prediction_val = float(prediction_result[0])
        else:
            prediction_val = float(prediction_result)

        formatted_time = pd.to_datetime(reading_time, unit='us')

        df = pd.DataFrame({
            'reading_time': [formatted_time],
            'prediction': [prediction_val]
        })

        df.to_sql(name=os.getenv("PREDICTION_TABLE_NAME"), con=engine, if_exists="append", index=False)

        logging.info("Prediction result inserted successfully with result: %s and reading_time: %s", prediction_result, reading_time)
    except Exception as e:
        logging.error("Error occurred while inserting prediction result: %s", e)


if __name__ == "__main__":
    try:
        kafka_conf = {
            'bootstrap.servers': os.getenv("BOOTSTRAP_SERVER"),
            'group.id': 'my_group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
        logging.info("Starting to ingest input data from Kafka topic.")
        kafka_input = KafkaInput(topic=os.getenv("KAFKA_TOPIC"), conf=kafka_conf)

        logging.info("Loading ML Model from MLflow...")
        mlflow.set_tracking_uri(os.getenv("MLFLOW_SERVER"))
        model_uri = f"models:/{os.getenv('REGISTERD_MODEL')}@production"
        loaded_model = mlflow.pyfunc.load_model(model_uri)
        logging.info("Model loaded successfully. Starting stream...")

        while True:
            msg = kafka_input.get_single_message()

            if msg is not None:
                logging.info("New row received from Kafka topic...")
                print('message received')
                data = processed_data(msg)
                logging.info("feature data used to prediction %s", data.columns)

                reading_time_val = None
                if "reading_time" in data.columns:
                    reading_time_series = data.pop('reading_time')
                    reading_time_val = reading_time_series.iloc[0] if not reading_time_series.empty else None

                # Remove target variable if it leaked into the stream
                if 'real_output' in data.columns:
                    data.pop('real_output')

                prediction_result = inference(data, loaded_model)

                if reading_time_val:
                    insert_data(reading_time_val, prediction_result)

                else:
                    logging.warning("Reading time is missing in the processed data. Skipping database insertion.")

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
