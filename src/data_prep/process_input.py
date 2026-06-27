import logging

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    colorize=True,
)


def process_input(msg: dict) -> pd.DataFrame:
    try:
        logging.info("Processing input data...")
        json_data = msg['payload']['after']
        if not json_data:
            logging.warning("Received message does not contain 'after' data. Skipping processing.")
            return pd.DataFrame()
        pd_data = pd.DataFrame([json_data])
        data = pd_data.drop(columns=['No', 'station'], errors='ignore')

        if "wd" in data.columns:
            data = pd.get_dummies(data, columns=['wd'])

        logging.info("Input data processed successfully.")
        return data
    except Exception as e:
        logging.error("Error occurred while processing input data: %s", e)
        raise e
