from datetime import datetime
from dotenv import load_dotenv
from hyperopt import STATUS_OK, Trials, fmin, hp, tpe
from hyperopt.pyll import scope
import mlflow
import os
import pandas as pd
from prefect import task, flow, get_run_logger
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import root_mean_squared_error
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from src.model_managment.regester_model import register_best_model
load_dotenv()

mlflow.set_tracking_uri(os.getenv('MLFLOW_SERVER'))
mlflow.set_experiment(os.getenv('MLFLOW_EXPERIMENT_NAME'))


@task(name="fetch_data", retries=3, retry_delay_seconds=10)
def fetch_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    logger = get_run_logger()
    try:
        engine = create_engine(f"postgresql+psycopg://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}/{os.getenv('POSTGRES_DB')}")
        
        with Session(engine) as session:
            query = text(f"""
                SELECT so2, no2, co, o3, temp, pres, dewp, wspm, wd, real_output
                FROM {os.getenv("TABLE_NAME")}
            """)
            result = session.execute(query)
            data = result.fetchall()
            
        logger.info(f"Successfully fetched data: {len(data)}")
        
        df = pd.DataFrame(data, columns=result.keys())
        df = pd.get_dummies(df, columns=['wd'])
        
        threshold = int(len(df) * 0.1)
        test_df = df[:threshold].copy()
        train_df = df[threshold:].copy()
        return train_df, test_df
    except Exception as e:
        logger.error(f"Faild to fetch data due to {e}")
        raise

@task(name="split_to_x_y", retries=5, retry_delay_seconds=10)
def input_output_split(df: pd.DataFrame):
    logger = get_run_logger()
    try:
        logger.info("Start splitting data to feature + output...")
        new_df = df.copy()
        y = new_df.pop("real_output")
        x = new_df
        logger.info("Successfully splited data")
        return x, y
    except Exception as e:
        logger.error(f"Couldn't split data due to {e}")
        raise


@task(name="Training_models", retries=5, retry_delay_seconds=10)
def run_optmization(num_trials: int, X_train: pd.DataFrame, Y_train: pd.DataFrame, X_test: pd.DataFrame, Y_test: pd.DataFrame) -> None:
    """
    Run the hyperparameter optimization process using Hyperopt.
    Parameters:
        num_trials (int): The number of trials to perform during the optimization process.
        file (str): The name of the CSV file containing the data.
    Returns:
        None
    """
    logging = get_run_logger()
    try:
        logging.info("Starting Processing data...")
        now = datetime.now()
        current_year = now.year
        current_month = now.month
        def objective(params):
            logging.info(f"Running trials...")
            try:
                with mlflow.start_run(name=f"Run_{current_month}_{current_year}"):
                    mlflow.log_params(params)
                    model = RandomForestRegressor(**params)
                    model.fit(X_train, Y_train)
                    preds = model.predict(X_test)
                    rmse = root_mean_squared_error(Y_test, preds)
                    mlflow.log_metric("rmse", rmse)
                    mlflow.sklearn.log_model(model, "model")

                logging.info(f"Trial completed with RMSE: {rmse}")
                return {'loss': rmse, 'status': STATUS_OK}
            
            # Stack execption so the one we can ignore goes on but the Fatal ones stop the sytem to save resources and time

            # except (ValueError, KeyError) as e:
            #     logging.critical(f"FATAL DATA ERROR: {e}. Cannot continue training.")
            #     raise e  # KILLS THE SCRIP

            # except requests.exceptions.ConnectionError as e:
            #     logging.warning(f"Could not connect to MLflow: {e}. Skipping trial.")
            #     return {'loss': float('inf'), 'status': STATUS_FAIL}
                
            
            # except MemoryError as e:
            #     logging.warning(f"Memory error during model training: {e}. Skipping trial.")
            #     return {'loss': float('inf'), 'status': STATUS_FAIL}
            
            except Exception as e:
                logging.error(f"Error occurred during objective function execution: {e}")
                raise e
                # return {'loss': float('inf'), 'status': STATUS_FAIL}
        
        space = {
            'n_estimators': scope.int(hp.quniform('n_estimators', 50, 200, 10)),
            'max_depth': scope.int(hp.quniform('max_depth', 5, 30, 1)),
            'min_samples_split': scope.int(hp.quniform('min_samples_split', 2, 10, 1)),
            'min_samples_leaf': scope.int(hp.quniform('min_samples_leaf', 1, 5, 1)),
            'bootstrap': hp.choice('bootstrap', [True, False])
        }

        trials = Trials()
        
        fmin(
            fn=objective, space=space,
            algo=tpe.suggest, max_evals=num_trials,
            trials=trials
        )
        
        logging.info(f"Optimization completed with {num_trials} trials.")
            
    except Exception as e:
        logging.error("Error occurred while processing data: %s", str(e))
        return


@task(name="evaluate_and_register")
def register_task():
    logger = get_run_logger()
    try:
        logger.info("Starting evaluation against golden dataset...")
        register_best_model()
        logger.info("Evaluation Complete")
    except Exception as e:
        logger.error(f"Faild to evaluate and register model due to: {e} ")
        raise

@flow(name="main_flow", retries=5, retry_delay_seconds=10)
def main():
    logger = get_run_logger()
    try:
        logger.info("Start the main flow...")
        train, test = fetch_data()
        x_train, y_train = input_output_split(train)
        x_test, y_test = input_output_split(test)
        run_optmization(100, x_train, y_train, x_test, y_test)
        register_task()
        logger.info("Successfully finish training FLow")
    except Exception as e:
        logger.error(f"Main Flow faild due to {e}")
        raise

if __name__ == "__main__":
    
    main.from_source(
        source="/opt/prefect/app",
        entrypoint="src/pipeline/retrain_flow.py:main"
    ).deploy(
        name="automated-retrain-deployment",
        work_pool_name="my-process-pool"
        # cron="*/1 * * * *"
    )
# run_optmization(num_trials=100, file=file)