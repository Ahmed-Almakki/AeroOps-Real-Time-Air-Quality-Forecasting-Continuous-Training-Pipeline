import os
import logging
from prefect import task, get_run_logger
import mlflow
import pandas as pd
from dotenv import load_dotenv
from mlflow.entities import ViewType
from sklearn.metrics import root_mean_squared_error
import sys


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
load_dotenv()

@task(name="register_best_model", retries=3, retry_delay_seconds=10)
def register_best_model(client, exp_name, run_name) -> None:
    """
    Register the best model(s) from the MLflow experiment based on the lowest RMSE metric.
    Parameters:
        top_n (int): The number of top models to register based on RMSE. Default is 1.
        delete_unwanted (bool): Whether to delete unwanted runs. Default is True.
    """
    logger = get_run_logger()
    try:
        logger.info("Start the Process of Registring the model...")
        experiment = client.get_experiment_by_name(exp_name)
        if experiment is None:
            logger.error("Experiment not found.")
            return

        experiment_id = experiment.experiment_id

        runs = client.search_runs(
            experiment_id,
            run_view_type=ViewType.ACTIVE_ONLY,
            order_by=["metrics.rmse ASC"],
            filter_string=f"tags.`mlflow.runName` = '{run_name}'",
        )

        if not runs:
            logger.warning("No runs found in experiment %s", experiment_id)
            return

        if len(runs) > 1:
            logger.info("Multiple runs found. Start selecting the best model based on RMSE.")

        run_id = runs[0].info.run_id
        model_uri = f"runs:/{run_id}/model"
        new_model_check = compare_models(run_id, client)
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
            logger.info("The new model is registerd as production")
        else:
            logger.warning("The old model Still the production model")

        # delete_unwanted_runs(run_ids_to_save, runs, client) if delete_unwanted else logger.info("Unwanted runs will not be deleted.")
    except Exception as e:
        logger.error("Error occurred while registering the best model: %s", e)


@task(name="Comparing_the_models", retries=3, retry_delay_seconds=10)
def compare_models(best_run_id, client) -> bool:
    logger = get_run_logger()
    try:
        logger.info("Start comparing the two models...")
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
            logger.warning("Could not load old production model due to %s.", e)
            return True

        # comparing both models
        if new_model_result < old_model_result:
            logger.info("The new model is performing better than the old one")
            check_archive = archive_old_model(client)

            if check_archive:
                logger.info("Old model registerd as archive")
                return True

            logger.error(
                "Faild to register old model as archive even though the new model is performing better.\nold_model result: %s\tnew_model result: %s",
                old_model_result,
                new_model_result,
            )
            return False

        logger.warning(
            "Old model is performing better than the new trained one.\nold_model_result: %s\tnew_model_result: %s",
            old_model_result,
            new_model_result,
        )
        return False

    except Exception as e:
        logger.error("Faild to compare the two models due to %s", e)
        raise


@task(name="process_golden_data", retries=3, retry_delay_seconds=10)
def process_golden_data(df):
    logger = get_run_logger()
    EXPECTED_FEATURES = [
    'so2', 'no2', 'co', 'o3', 'temp', 'pres', 'dewp', 'rain', 'wspm',
    'wd_E', 'wd_ENE', 'wd_ESE', 'wd_N', 'wd_NE', 'wd_NNE', 'wd_NNW',
    'wd_NW', 'wd_S', 'wd_SE', 'wd_SSE', 'wd_SSW', 'wd_SW', 'wd_W',
    'wd_WNW', 'wd_WSW', 'real_output'
]
    try:
        logger.info("Start Processing Golden Dataset...")

        df = df.rename(columns={'pm2.5': 'real_output'})
        df = df.dropna()

        if "wd" in df.columns:
            df = pd.get_dummies(df, columns=['wd'])

        df = df.reindex(columns=EXPECTED_FEATURES, fill_value=0)
        logger.info("Successfully Finish Processing Golden Dataset, result with length of %s", len(df))
        return df
    except Exception as e:
        logger.error("Faild to process golden dataset due to %s", e)
        raise e


@task(name="predict", retries=3, retry_delay_seconds=10)
def predict(model) -> float:
    logger = get_run_logger()
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(current_dir, "golden_dataset.csv")
        try:
            golden_df = pd.read_csv(csv_path)
            processed_df = process_golden_data(golden_df)
        except Exception as e:
            logger.error("Faild to load golden data set due to %s", e)
        y = processed_df.pop("real_output")

        y_predict = model.predict(processed_df)
        rmse = root_mean_squared_error(y, y_predict)
        return rmse
    except Exception as e:
        logger.error("Faild to predict due to %s", e)
        raise e

@task(name="archive_old_model", retries=3, retry_delay_seconds=10)
def archive_old_model(client) -> bool:
    logger = get_run_logger()
    try:
        logger.info("Initiating archive process for the old model...")
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
        logger.error("Cant set alias to the old model due to: %s", e)
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
#         logger.warning("No run IDs provided for deletion.")
#         return
#     try:
#         print(f"type{type(client)}\t\t{type(run_ids)}")
#         for run in runs:
#             if run.info.run_id not in run_ids:
#                 client.delete_run(run.info.run_id)
#     except Exception as e:
#         logging.error(f"Error occurred while deleting runs: {e}")

# run_id = register_best_model(top_n=2, delete_unwanted=True)
