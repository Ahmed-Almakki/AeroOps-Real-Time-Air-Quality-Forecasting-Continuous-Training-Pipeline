import os
import logging
from datetime import datetime

import mlflow
import pandas as pd
from dotenv import load_dotenv
from mlflow import MlflowClient
from mlflow.entities import ViewType
from sklearn.metrics import root_mean_squared_error

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

client = MlflowClient()
now = datetime.now()
current_month = now.month
current_year = now.year
run_name = f"Run_{current_month}_{current_year}"


def get_experment():
    try:
        mlflow.set_tracking_uri(os.getenv('MLFLOW_SERVER'))
        experiment_name = os.getenv('MLFLOW_EXPERIMENT_NAME')
        experiment = client.get_experiment_by_name(experiment_name)
        return experiment
    except Exception as e:
        logging.error("Couldn't get experiment due to %s", e)
        return None


def register_best_model() -> None:
    """
    Register the best model(s) from the MLflow experiment based on the lowest RMSE metric.
    Parameters:
        top_n (int): The number of top models to register based on RMSE. Default is 1.
        delete_unwanted (bool): Whether to delete unwanted runs. Default is True.
    """
    try:
        logging.info("Start the Process of Registring the model...")
        experiment = get_experment()
        if experiment is None:
            logging.error("Experiment not found.")
            return

        experiment_id = experiment.experiment_id

        runs = client.search_runs(
            experiment_id,
            run_view_type=ViewType.ACTIVE_ONLY,
            order_by=["metrics.rmse ASC"],
            filter_string=f"tags.`mlflow.runName` = '{run_name}'",
        )

        if not runs:
            logging.warning("No runs found in experiment %s", experiment_id)
            return

        if len(runs) > 1:
            logging.info("Multiple runs found. Start selecting the best model based on RMSE.")

        run_id = runs[0].info.run_id
        model_uri = f"runs:/{run_id}/model"
        new_model_check = compare_models(run_id)
        if new_model_check:
            registerd_model = mlflow.register_model(
                model_uri=model_uri, name=os.getenv("REGISTERD_MODEL")
            )
            registerd_model_version = registerd_model.version
            client.set_registered_model_alias(
                name=os.getenv("REGISTERD_MODEL"),
                alias="production",
                version=registerd_model_version,
            )
            logging.info("The new model is registerd as production")
        else:
            logging.warning("The old model Still the production model")

        # delete_unwanted_runs(run_ids_to_save, runs, client) if delete_unwanted else logging.info("Unwanted runs will not be deleted.")
    except Exception as e:
        logging.error("Error occurred while registering the best model: %s", e)


def compare_models(best_run_id) -> bool:
    try:
        logging.info("Start comparing the two models...")
        model_name = os.getenv("REGISTERD_MODEL")
        alias = "production"

        # New model prediction step
        new_model_uri = f"runs:/{best_run_id}/model"
        new_model = mlflow.pyfunc.load_model(new_model_uri)

        new_model_result = predict(new_model)

        # old model prediction step with safty of first time when thre is no old model
        try:
            old_model_uri = f"models:/{model_name}@{alias}"
            old_model = mlflow.pyfunc.load_model(old_model_uri)

            old_model_result = predict(old_model)
        except Exception as e:
            logging.warning("Could not load old production model due to %s.", e)
            return True

        # comparing both models
        if new_model_result < old_model_result:
            logging.info("The new model is performing better than the old one")
            check_archive = archive_old_model()

            if check_archive:
                logging.info("Old model registerd as archive")
                return True

            logging.error(
                "Faild to register old model as archive even though the new model is performing better.\nold_model result: %s\tnew_model result: %s",
                old_model_result,
                new_model_result,
            )
            return False

        logging.warning(
            "Old model is performing better than the new trained one.\nold_model_result: %s\tnew_model_result: %s",
            old_model_result,
            new_model_result,
        )
        return False

    except Exception as e:
        logging.error("Faild to compare the two models due to %s", e)
        raise


def predict(model) -> float:
    try:
        golden_df = pd.read_csv("./golden_dataset.csv")
        y = golden_df.pop("PM2.5")
        y_predict = model.predict(golden_df)
        rmse = root_mean_squared_error(y, y_predict)
        return rmse
    except Exception as e:
        raise e


def archive_old_model() -> bool:
    try:
        logging.info("Initiating archive process for the old model...")
        old_model = client.get_model_version_by_alias(
            name=os.getenv("REGISTERD_MODEL"), alias="production"
        )
        old_model_version = old_model.version

        client.set_registered_model_alias(
            name=os.getenv("REGISTERD_MODEL"),
            alias="archive",
            version=old_model_version,
        )
        return True
    except Exception as e:
        logging.error("Cant set alias to the old model due to: %s", e)
        raise


# def delete_unwanted_runs(run_ids: list, runs: list, client: MlflowClient) -> None:
#     """
#     Delete unwanted runs from the MLflow experiment.
#     Parameters:
#         run_ids (list): A list of run IDs to be deleted.
#         runs (list): A list of all runs in the experiment.
#         client: (MlflowClient) client of mlflow to delete runs.
#     Returns:
#         None
#     """
#     if not run_ids:
#         logging.warning("No run IDs provided for deletion.")
#         return
#     try:
#         print(f"type{type(client)}\t\t{type(run_ids)}")
#         for run in runs:
#             if run.info.run_id not in run_ids:
#                 client.delete_run(run.info.run_id)
#     except Exception as e:
#         logging.error(f"Error occurred while deleting runs: {e}")

# run_id = register_best_model(top_n=2, delete_unwanted=True)
