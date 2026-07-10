from datetime import datetime
import pytest
from unittest.mock import patch
import time
import pandas as pd
import tempfile
import mlflow
from mlflow import MlflowClient
from prefect.testing.utilities import prefect_test_harness
import os
from dotenv import load_dotenv
import importlib

import src.model_managment.regester_model as register_module

load_dotenv()

@pytest.fixture(autouse=True)
def prefct_test_fixture():
    with prefect_test_harness():
        yield


class DummyModel(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input):
        return [10.5, 12.0]


class PreviousDummyModel(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input):
        return [0] * len(model_input)


@pytest.fixture(scope="function", autouse=True)
def isolated_mlflow(monkeypatch):
    with tempfile.TemporaryDirectory() as temp_dir:
        monkeypatch.setenv("REGISTERD_MODEL", "test_model")
        importlib.reload(register_module)

        dummy_data = pd.DataFrame({"pm2.5": [10.5, 12.0], "feature1": [1, 2]})

        # 3. Intercept read_csv! Now your code will NEVER read the 143-row real file during tests.
        monkeypatch.setattr(register_module.pd, "read_csv", lambda _: dummy_data)
        db_path = os.path.join(temp_dir, "mlflow.db")
        mlflow.set_tracking_uri(f"sqlite:///{db_path}")

        artifact_path = os.path.join(temp_dir, "mlruns")

        experiment_id = mlflow.create_experiment(
            "test_experiment",
            artifact_location=f"file://{artifact_path}"
        )

        mlflow.set_experiment(experiment_id=experiment_id)

        yield

def test_register_best_model():
    client = MlflowClient()

    run_name = f"Retrain_{datetime.now().strftime('%m_%d_%H:%M:%S')}"
    with mlflow.start_run(run_name=run_name):
        mlflow.pyfunc.log_model(artifact_path="model", python_model=DummyModel())
        mlflow.log_metric("rmse", 0.5)

    register_module.register_best_model(client=client, exp_name="test_experiment", run_name=run_name)


    registered_models = client.search_registered_models()

    assert any(model.name == "test_model" for model in registered_models), "Model was not registered."



def test_register_best_model_new_better(caplog):
    client = MlflowClient()
    caplog.set_level("INFO")

    with mlflow.start_run(run_name=f"Retrain_{datetime.now().strftime('%m_%d_%H:%M:%S')}") as previous_run:
        previous_run_id = previous_run.info.run_id
        mlflow.pyfunc.log_model(artifact_path="model", python_model=PreviousDummyModel(), registered_model_name=os.getenv("REGISTERD_MODEL"))
        mlflow.log_metric("rmse", 1.0)
    client.set_registered_model_alias(name=os.getenv("REGISTERD_MODEL"), alias="production", version="1")

    time.sleep(20)
    run_name = f"Retrain_{datetime.now().strftime('%m_%d_%H:%M:%S')}"
    with mlflow.start_run(run_name=run_name) as current_run:
        current_run_id = current_run.info.run_id
        mlflow.pyfunc.log_model(artifact_path="model", python_model=DummyModel())
        mlflow.log_metric("rmse", 0.5)
        mlflow.set_tag("mlflow.runName", run_name)

    register_module.register_best_model(client=client, exp_name="test_experiment", run_name=run_name)

    model_name = os.getenv("REGISTERD_MODEL")

    prod_version = client.get_model_version_by_alias(name=model_name, alias="production")
    assert prod_version.run_id == current_run_id, "New model should be aliased as production."

    archive_version = client.get_model_version_by_alias(name=model_name, alias="archive")
    assert archive_version.run_id == previous_run_id, "Old model should be aliased as archive."


def test_register_best_model_old_better(caplog):
    client = MlflowClient()
    caplog.set_level("INFO")

    with mlflow.start_run(run_name=f"Retrain_{datetime.now().strftime('%m_%d_%H:%M:%S')}"):
        mlflow.pyfunc.log_model(artifact_path="model", python_model=DummyModel(), registered_model_name=os.getenv("REGISTERD_MODEL"))
        mlflow.log_metric("rmse", 0.5)
    client.set_registered_model_alias(name=os.getenv("REGISTERD_MODEL"), alias="production", version="1")

    time.sleep(20)
    run_name = f"Retrain_{datetime.now().strftime('%m_%d_%H:%M:%S')}"
    with mlflow.start_run(run_name=run_name) as current_run:
        mlflow.pyfunc.log_model(artifact_path="model", python_model=PreviousDummyModel())
        mlflow.log_metric("rmse", 1.0)
        mlflow.set_tag("mlflow.runName", run_name)

    register_module.register_best_model(client=client, exp_name="test_experiment", run_name=run_name)


    assert "The old model Still the production model" in caplog.messages
