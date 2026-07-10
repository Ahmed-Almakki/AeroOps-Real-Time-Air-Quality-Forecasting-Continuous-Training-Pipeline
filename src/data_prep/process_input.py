import logging

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

EXPECTED_FEATURES = [
    'so2', 'no2', 'co', 'o3', 'temp', 'pres', 'dewp', 'rain', 'wspm',
    'wd_E', 'wd_ENE', 'wd_ESE', 'wd_N', 'wd_NE', 'wd_NNE', 'wd_NNW',
    'wd_NW', 'wd_S', 'wd_SE', 'wd_SSE', 'wd_SSW', 'wd_SW', 'wd_W',
    'wd_WNW', 'wd_WSW'
]


def process_input(msg: dict) -> pd.DataFrame:
    try:
        logging.info("Processing input data...")
        json_data = msg['payload']['after']
        if not json_data:
            logging.warning("Received message does not contain 'after' data. Skipping processing.")
            return pd.DataFrame()
        pd_data = pd.DataFrame([json_data])
        data = pd_data.drop(columns=['No', 'created', 'updated', 'id'], errors='ignore')

        if "wd" in data.columns:
            data = pd.get_dummies(data, columns=['wd'])

        reading_time = data['reading_time'] if 'reading_time' in data.columns else None
        data = data.reindex(columns=EXPECTED_FEATURES, fill_value=0)

        if reading_time is not None:
            data['reading_time'] = reading_time

        logging.info("Input data processed successfully.")
        return data
    except Exception as e:
        logging.error("Error occurred while processing input data: %s", e)
        raise e
