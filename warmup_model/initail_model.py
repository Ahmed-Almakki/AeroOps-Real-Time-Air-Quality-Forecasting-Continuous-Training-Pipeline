from datetime import datetime
from dotenv import load_dotenv
import os
import mlflow
from pathlib import Path
from mlflow import MlflowClient
import pandas as pd
from sklearn.ensemble import RandomForestRegressor

from src.data_prep.process_input import process_input

load_dotenv()

def read_file(file):
    try:
        df = pd.read_csv(file)
        df = df.drop(columns=['No', 'station', 'PM10', 'year', 'month', 'day', 'hour'])
        df = df.dropna()
        if "wd" in df.columns:
            df = pd.get_dummies(df, columns=['wd'])
        y = df.pop('PM2.5')
        return df, y
    except Exception as e:
        raise e


def train(x, y, client):
    try:
        now = datetime.now()
        with mlflow.start_run(run_name=f"Run_{now.month}_{now.year}"):
            model = RandomForestRegressor(n_estimators=5, max_depth=5, min_samples_leaf=1, min_samples_split=2, bootstrap=True)
            model.fit(x, y)
            mlflow.sklearn.log_model(model, "model", registered_model_name=os.getenv("REGISTERD_MODEL"))
        client.set_registered_model_alias(name=os.getenv("REGISTERD_MODEL"), alias="production", version="1")
    except Exception as e:
        raise e



def main():
    mlflow.set_tracking_uri("http://localhost:8082")
    mlflow.set_experiment(os.getenv("MLFLOW_EXPERIMENT_NAME"))
    client = MlflowClient()
    try:
        file = "PRSA_Data_Guanyuan_20130301-20170228.csv"

        project_root = Path(__file__).resolve().parent.parent
        file_path = project_root / 'warmup_model' / file
        print(f"file path {file_path}")
        print("reading file....")
        x, y = read_file(file_path)
        print("Start training...")
        train(x, y, client)
        print("Successfully trained model")
    except Exception as e:
        raise e

if __name__ == "__main__":
    main()
