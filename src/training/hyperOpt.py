from hyperopt import STATUS_OK, Trials, fmin, hp, tpe, STATUS_FAIL
from hyperopt.pyll import scope
import logging
import mlflow
import pandas
import requests
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import root_mean_squared_error

from src.data_prep.eda import get_processed_data


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
mlflow.set_tracking_uri("http://127.0.0.1:8080")
mlflow.set_experiment("Air Pollution Prediction Hyperopt Experiment")


def dataframe_selcetion(df: pandas.DataFrame) -> pandas.DataFrame:
    """
    Select the target variable 'pm2.5' and the feature variables from the DataFrame.
    Parameters:
        df (pandas.DataFrame): The input DataFrame containing the data.
    Returns:
        pandas.DataFrame: A tuple containing the feature variables (X_train) 
        and the target variable (Y_train).
    """
    try:
        Y_train = df['PM2.5']
        X_train = df.drop(columns=['PM2.5'])

        logging.info("Dataframe selection successful.")
        return X_train, Y_train
    except Exception as e:
        logging.error(f"Error occurred during dataframe selection: {e}")
        return None
    

def run_optmization(num_trials: int, file: str) -> None:
    """
    Run the hyperparameter optimization process using Hyperopt.
    Parameters:
        num_trials (int): The number of trials to perform during the optimization process.
        file (str): The name of the CSV file containing the data.
    Returns:
        None
    """
    try:
        logging.info(f"Starting Processing data from file: {file}")
        df_train, df_b, df_test = get_processed_data(file)
        
        Y_test = df_test['PM2.5']
        X_test = df_test.drop(columns=['PM2.5'])

        for df in [df_train, df_b]:
            X_train, Y_train = dataframe_selcetion(df)
            if X_train is None or Y_train is None:
                logging.error("Dataframe selection failed. Skipping optimization.")
                continue

            def objective(params):
                logging.info(f"Running trials...")
                try:
                    with mlflow.start_run():
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

                except (ValueError, KeyError) as e:
                    logging.critical(f"FATAL DATA ERROR: {e}. Cannot continue training.")
                    raise e  # KILLS THE SCRIP

                except requests.exceptions.ConnectionError as e:
                    logging.warning(f"Could not connect to MLflow: {e}. Skipping trial.")
                    return {'loss': float('inf'), 'status': STATUS_FAIL}
                
                except MemoryError as e:
                    logging.warning(f"Memory error during model training: {e}. Skipping trial.")
                    return {'loss': float('inf'), 'status': STATUS_FAIL}
                
                except Exception as e:
                    logging.error(f"Error occurred during objective function execution: {e}")
                    return {'loss': float('inf'), 'status': STATUS_FAIL}
            
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


file = 'PRSA_Data_Aotizhongxin_20130301-20170228.csv'
run_optmization(num_trials=100, file=file)