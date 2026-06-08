from prefect import flow, task
from prefect.deployments import run_deployment
import time

@flow(name="Flow-2-Daily-Drift")
def daily_drift():
    print("Fetching the last 24 rows of sensor data from the Database...")
    # (Your code to fetch data from your DB container)
    
    print("Checking for data drift using Evidently UI...")
    drift_detected = True  # Let's assume Evidently found drift
    
    if drift_detected:
        print("Alert! Drift found. Triggering Flow 3...")
        # This tells the server to immediately put Flow 3 into the work pool
        run_deployment(name="Flow-3-MLflow-Retrain/automated-retrain", timeout=0)

@flow(name="Flow-3-MLflow-Retrain")
def mlflow_retrain():
    print("Connecting to MLflow container...")
    print("Looping through 100 model combinations...")
    # (Your heavy loop to train models, track parameters, and pick the best one)

if __name__ == "__main__":
    # This is what Service 3 executes to register BOTH flows automatically
    daily_drift.deploy(
        name="automated-drift-check",
        work_pool_name="my-process-pool",
        cron="0 0 * * *"  # Run automatically every 24 hours
    )
    mlflow_retrain.deploy(
        name="automated-retrain",
        work_pool_name="my-process-pool"  # No cron! This only runs when Flow 2 calls it
    )