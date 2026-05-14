import logging
import os
from dotenv import load_dotenv
import mlflow
from mlflow import MlflowClient
from mlflow.entities import ViewType


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()
mlflow.set_tracking_uri(os.getenv('MLFLOW_SERVER'))

def register_best_model(top_n: int = 1) -> None:
    """
    Register the best model(s) from the MLflow experiment based on the lowest RMSE metric.
    Parameters:
        top_n (int): The number of top models to register based on RMSE. Default is 1.
    Returns:
        None
    """
    try:
        client = MlflowClient()
        experiment_name = os.getenv('MLFLOW_EXPERIMENT_NAME')
        experiment = client.get_experiment_by_name(experiment_name)

        if experiment is None:
            logging.error(f"Experiment '{experiment_name}' not found.")
            return
        
        experiment_id = experiment.experiment_id
        num_runs = 1

        runs = client.search_runs(
            experiment_id, run_view_type=ViewType.ACTIVE_ONLY,
            order_by=["metrics.rmse ASC"], max_results=top_n
        )
        
        if not runs:
            logging.warning(f"No runs found in experiment '{experiment_name}'.")
            return
        
        if len(runs) > 1:
            logging.info(f"Multiple runs found. Start selecting the best model based on RMSE.")
            num_runs = len(runs)
        
        # if there are multiple runs, we choose all of them and register them as best models
        i = 0
        while i < num_runs:
            run_id = runs[i].info.run_id
            model_uri = f"runs:/{run_id}/model"
            mlflow.register_model(model_uri, "Best Air Pollution Prediction Model")
            logging.info(f"Best model registered successfully with run ID: {run_id}")
            i += 1

        # In case if you want to train them and choose the best model
        # for run in runs:
            # traininig_model(run)
    except Exception as e:
        logging.error(f"Error occurred while registering the best model: {e}")

register_best_model(top_n=1)