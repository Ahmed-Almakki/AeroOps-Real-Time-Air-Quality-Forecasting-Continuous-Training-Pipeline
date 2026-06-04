import logging
import os
from dotenv import load_dotenv
import mlflow
from mlflow import MlflowClient
from mlflow.entities import ViewType


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()


def register_best_model(top_n: int = 3, delete_unwanted: bool = True) -> None:
    """
    Register the best model(s) from the MLflow experiment based on the lowest RMSE metric.
    Parameters:
        top_n (int): The number of top models to register based on RMSE. Default is 1.
        delete_unwanted (bool): Whether to delete unwanted runs. Default is True.
    """
    try:
        mlflow.set_tracking_uri(os.getenv('MLFLOW_SERVER'))
        experiment_name = os.getenv('MLFLOW_EXPERIMENT_NAME')

        client = MlflowClient()
        experiment = client.get_experiment_by_name(experiment_name)
        if experiment is None:
            logging.error(f"Experiment '{experiment_name}' not found.")
            return
        
        experiment_id = experiment.experiment_id
        num_runs = 1

        runs = client.search_runs(
            experiment_id, run_view_type=ViewType.ACTIVE_ONLY,
            order_by=["metrics.rmse ASC"]
        )
        
        if not runs:
            logging.warning(f"No runs found in experiment '{experiment_name}'.")
            return
        
        if len(runs) > 1:
            logging.info(f"Multiple runs found. Start selecting the best model based on RMSE.")
            num_runs = top_n
        
        # if there are multiple runs, we choose all of them and register them as best models
        i = 0
        run_ids_to_save = []
        while i < num_runs:
            run_id = runs[i].info.run_id
            model_uri = f"runs:/{run_id}/model"
            mlflow.register_model(model_uri, "Best Air Pollution Prediction Model")
            logging.info(f"Best model registered successfully with run ID: {run_id}")
            run_ids_to_save.append(run_id)
            i += 1
        delete_unwanted_runs(run_ids_to_save, runs, client) if delete_unwanted else logging.info("Unwanted runs will not be deleted.")

        # In case if you want to train them and choose the best model
        # for run in runs:
            # traininig_model(run)
    except Exception as e:
        logging.error(f"Error occurred while registering the best model: {e}")


def delete_unwanted_runs(run_ids: list, runs: list, client: MlflowClient) -> None:
    """
    Delete unwanted runs from the MLflow experiment.
    Parameters:
        run_ids (list): A list of run IDs to be deleted.
        runs (list): A list of all runs in the experiment.
        client: (MlflowClient) client of mlflow to delete runs.
    Returns:
        None
    """
    if not run_ids:
        logging.warning("No run IDs provided for deletion.")
        return
    try:
        print(f"type{type(client)}\t\t{type(run_ids)}")
        for run in runs:
            if run.info.run_id not in run_ids:
                client.delete_run(run.info.run_id)
    except Exception as e:
        logging.error(f"Error occurred while deleting runs: {e}")



run_id = register_best_model(top_n=2, delete_unwanted=True)