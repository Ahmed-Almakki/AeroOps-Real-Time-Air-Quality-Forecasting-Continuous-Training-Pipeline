from datetime import datetime
import pytest
import pandas as pd
import tempfile
import mlflow
from mlflow import MlflowClient
import os
from dotenv import load_dotenv
from src.model_managment.regester_model import register_best_model

load_dotenv()

class DummyModel(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input):
        return [10.5, 12.0]


class PreviousDummyModel(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input):
        return [0] * len(model_input)


@pytest.fixture(scope="function", autouse=True)
def isolated_mlflow():
    with tempfile.TemporaryDirectory() as temp_dir:
        os.environ["REGISTERD_MODEL"] = "test_model"
        db_path = os.path.join(temp_dir, "mlflow.db")
        mlflow.set_tracking_uri(f"sqlite:///{db_path}")
        mlflow.set_experiment("test_experiment")

        dummy_data = pd.DataFrame({"PM2.5": [10.5, 12.0], "feature1": [1, 2]})
        csv_path = "./golden_dataset.csv"
        dummy_data.to_csv(csv_path, index=False)

        yield

        if os.path.exists(csv_path):
            os.remove(csv_path)

def test_register_best_model():
    client = MlflowClient()

    now = datetime.now()
    with mlflow.start_run(run_name=f"Run_{now.month}_{now.year}"):
        mlflow.pyfunc.log_model(artifact_path="model", python_model=DummyModel())
        mlflow.log_metric("rmse", 0.5)

    register_best_model(client=client, exp_name="test_experiment")


    registered_models = client.search_registered_models()

    assert any(model.name == "test_model" for model in registered_models), "Model was not registered."



def test_register_best_model_new_better():
    client = MlflowClient()

    now = datetime.now()

    with mlflow.start_run(run_name=f"Run_{now.month - 1}_{now.year}") as previous_run:
        previous_run_id = previous_run.info.run_id
        mlflow.pyfunc.log_model(artifact_path="model", python_model=PreviousDummyModel(), registered_model_name=os.getenv("REGISTERD_MODEL"))
        mlflow.log_metric("rmse", 0.5)
    client.set_registered_model_alias(name=os.getenv("REGISTERD_MODEL"), alias="production", version="1")


    with mlflow.start_run(run_name=f"Run_{now.month}_{now.year}") as current_run:
        current_run_id = current_run.info.run_id
        mlflow.pyfunc.log_model(artifact_path="model", python_model=DummyModel())
        mlflow.log_metric("rmse", 0.5)
        mlflow.set_tag("mlflow.runName", f"Run_{now.month}_{now.year}")

    register_best_model(client=client, exp_name="test_experiment")

    model_name = os.getenv("REGISTERD_MODEL")

    prod_version = client.get_model_version_by_alias(name=model_name, alias="production")
    assert prod_version.run_id == current_run_id, "New model should be aliased as production."

    archive_version = client.get_model_version_by_alias(name=model_name, alias="archive")
    assert archive_version.run_id == previous_run_id, "Old model should be aliased as archive."
