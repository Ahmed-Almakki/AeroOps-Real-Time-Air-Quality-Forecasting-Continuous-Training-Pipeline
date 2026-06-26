import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def split_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    Split the 'date' column into separate 'year', 'month', 'day', and 'hour' columns.
    Parameters:
    df (pd.DataFrame): The input DataFrame containing a 'date' column.
    Returns:
        pd.DataFrame: A DataFrame with the 'date' column split into separate columns.
    """
    try:
        train_raw = df[df['year'] < 2016].copy()
        test_raw = df[df['year'] >= 2016].copy()
        df_test = test_raw.dropna().copy()
        logging.info("Date column split successfully.")
        return train_raw, df_test
    except Exception as e:
        logging.error("Error occurred while splitting date: %s", e)
        return df


def get_processed_data(file: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load and process the air pollution data from a CSV file.
    Parameters:
    file (str): The name of the CSV file containing the data.
    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: A tuple containing the processed DataFrame
    with filled missing values and a copy of the DataFrame with dropped missing values.
    """
    try:
        # current_dir = Path(os.getcwd())
        # file_path = current_dir / '../data' / file
        project_root = Path(__file__).resolve().parent.parent.parent
        file_path = project_root / 'data' / file
        logging.info("Loading data...")

        if not file_path.exists():
            logging.error("File %s does not exist.", file_path)
            return None, None, None

        df = pd.read_csv(file_path)
        df = df.drop(columns=['No', 'station'])
        df = pd.get_dummies(df, columns=['wd'])

        df_train, df_test = split_dataset(df)

        if df_train is None or df_test is None:
            logging.error("Failed to split dataset.")
            return None, None, None

        df_dropped_values = df_train.copy()

        df_train.fillna(method='ffill', inplace=True)
        df_dropped_values.dropna(inplace=True)

        logging.info("Data loaded and processed successfully.")
        return df_train, df_dropped_values, df_test
    except Exception as e:
        logging.error("Error occurred: %s", e)
        return None, None, None


# df, df_b, df_test = get_processed_data('PRSA_Data_Aotizhongxin_20130301-20170228.csv')
# print(f"df shape: {df.shape} | df_b shape: {df_b.shape} | df_test shape: {df_test.shape}")
