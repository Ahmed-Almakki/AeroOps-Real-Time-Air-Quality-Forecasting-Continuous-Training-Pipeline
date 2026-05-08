from hyperopt import STATUS_OK, Trials, fmin, hp, tpe
from hyperopt.pyll import scope
import logging
import mlflow
import pandas
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import root_mean_squared_error

from notebook.eda import get_processed_data


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
mlflow.set_tracking_uri("http://127.0.0.1:5000")
mlflow.set_experiment("random-forest-hyperopt")


def run_optmization(train: pandas.DataFrame, test: pandas.DataFrame, num_trials: int):
    try:
        file = 'PRSA_Data_Aotizhongxin_20130301-20170228.csv'
        df_train, df_b, df_test = get_processed_data(file)
        Y_train = df_train['pm2.5']
        X_train = df_train.drop(columns=['pm2.5'])
        Y_test = df_test['pm2.5']
        X_test = df_test.drop(columns=['pm2.5'])

        def objective(params):
            try:
                with mlflow.start_run():
                    mlflow.log_params(params)
                    model = RandomForestRegressor(**params)
                    model.fit(X_train, Y_train)
                    preds = model.predict(X_test)
                    rmse = root_mean_squared_error(Y_test, preds)
                    mlflow.log_metric("rmse", rmse)
                    mlflow.sklearn.log_model(model, "model")
                return {'loss': rmse, 'status': STATUS_OK}
            except Exception as e:
                logging.error(f"Error occurred during objective function execution: {e}")
                return {'loss': float('inf'), 'status': STATUS_OK}
            
    except Exception as e:
        logging.error("Error occurred while processing data: %s", str(e))
        logging.error("File not found")
        return


