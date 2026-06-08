import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def process_input(msg: dict) -> pd.DataFrame:
    try:
        logging.info("Processing input data...")
        json_data = msg['payload']['after']

        print(f"Received data for processing: {json_data}")
        pd_data = pd.DataFrame([json_data])
        data = pd_data.drop(columns=['No', 'station'])

        print(f"Data after dropping columns: {data}")
        data = pd.get_dummies(data, columns=['wd'])
        
        logging.info("Input data processed successfully.")
        return data
    except Exception as e:
        logging.error(f"Error occurred while processing input data: {e}")