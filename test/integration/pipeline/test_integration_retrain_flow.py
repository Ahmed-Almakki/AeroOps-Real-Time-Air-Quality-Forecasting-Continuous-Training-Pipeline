from testcontainers.postgres import PostgresContainer
import pytest
import psycopg2
from datetime import datetime, timedelta
import tempfile

from prefect.testing.utilities import prefect_test_harness

from src.pipeline.retrain_flow import main

@pytest.fixture(scope="function")
def postgres_container():
    with PostgresContainer("postgres:17.9") as postgres:
        user = postgres.username
        password = postgres.password
        db = postgres.dbname
        host = postgres.get_container_host_ip()
        port = postgres.get_exposed_port(5432)
        table_name = "sensor_data_test"

        conn = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            dbname=db
        )

        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                updated TIMESTAMP, rain FLOAT, so2 FLOAT, no2 FLOAT, co FLOAT, o3 FLOAT,
                temp FLOAT, pres FLOAT, dewp FLOAT, wspm FLOAT,
                real_output FLOAT, wd VARCHAR
            );
        """)

        now = datetime.now()
        for i in range(50):
            timestamp = now - timedelta(hours=i)
            cursor.execute(f"""
                INSERT INTO {table_name} VALUES (
                    '{timestamp}', 1.0, 1.0, 1.0, 1.0, 1.0, 20.0, 1000.0, 15.0, 2.0, 1.0, 'N'
                );
            """)
        conn.commit()
        conn.close()

        yield {
            "POSTGRES_USER": user,
            "POSTGRES_PASSWORD": password,
            "POSTGRES_HOST": host,
            "POSTGRES_PORT": str(port),
            "POSTGRES_DB": db,
            "TABLE_NAME": table_name
        }


@pytest.fixture(autouse=True)
def prefct_test_fixture():
    with prefect_test_harness():
        yield


def test_main(postgres_container, monkeypatch, caplog):
    caplog.set_level("INFO")

    monkeypatch.setenv("POSTGRES_USER", postgres_container["POSTGRES_USER"])
    monkeypatch.setenv("POSTGRES_PASSWORD", postgres_container["POSTGRES_PASSWORD"])
    monkeypatch.setenv("POSTGRES_PORT", postgres_container["POSTGRES_PORT"])
    monkeypatch.setenv("POSTGRES_HOST", postgres_container["POSTGRES_HOST"])
    monkeypatch.setenv("POSTGRES_DB", postgres_container["POSTGRES_DB"])
    monkeypatch.setenv("TABLE_NAME", postgres_container["TABLE_NAME"])
    monkeypatch.setenv("MLFLOW_EXPERIMENT_NAME", "test_experiment")

    # outside the with block the temporary directory will be deleted
    with tempfile.TemporaryDirectory() as temp_dir:
        # use local file system for mlflow tracking instead of a database or remote server
        monkeypatch.setenv("MLFLOW_SERVER", f"file://{temp_dir}")

        main(n_trails=1)

        assert "Successfully finish training FLow" in caplog.messages
