import psycopg2
from testcontainers.postgres import PostgresContainer
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from prefect.testing.utilities import prefect_test_harness

from src.pipeline.drift_check import daily_drift


def configuration(monkeypatch, postgres_container):
    monkeypatch.setenv("POSTGRES_USER", postgres_container["POSTGRES_USER"])
    monkeypatch.setenv("POSTGRES_PASSWORD", postgres_container["POSTGRES_PASSWORD"])
    monkeypatch.setenv("POSTGRES_HOST", postgres_container["POSTGRES_HOST"])
    monkeypatch.setenv("POSTGRES_DB", postgres_container["POSTGRES_DB"])
    monkeypatch.setenv("TABLE_NAME", postgres_container["TABLE_NAME"])
    monkeypatch.setenv("PREDICTION_TABLE_NAME", postgres_container["PREDICTION_TABLE_NAME"])
    monkeypatch.setenv("REPORTS_DIR", "/tmp")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "N/A")

    host, port = postgres_container["POSTGRES_HOST"].split(":")
    conn = psycopg2.connect(
        user=postgres_container["POSTGRES_USER"],
        password=postgres_container["POSTGRES_PASSWORD"],
        host=host,
        port=port,
        dbname=postgres_container["POSTGRES_DB"]
    )
    return conn


@pytest.fixture(scope="function")
def postgres_container():
    with PostgresContainer("postgres:17.9") as postgres:
        user = postgres.username
        password = postgres.password
        db = postgres.dbname
        host = postgres.get_container_host_ip()
        port = postgres.get_exposed_port(5432)
        host_and_port = f"{host}:{port}"
        table_name = "sensor_data_test"
        prediction_table_name = "air_pollution_predictions_test"

        conn = psycopg2.connect(user=user,
                password=password,
                host=host,
                port=port,
                dbname=db)
        cursor = conn.cursor()

        cursor.execute(f"""
            CREATE TABLE {table_name} (
                updated TIMESTAMP, so2 FLOAT, no2 FLOAT, co FLOAT, o3 FLOAT,
                temp FLOAT, pres FLOAT, dewp FLOAT, wspm FLOAT,
                reading_time TIMESTAMP, real_output FLOAT, wd VARCHAR
            );
        """)

        cursor.execute(f"""
            CREATE TABLE {prediction_table_name} (
                id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY NOT NULL,
                reading_time TIMESTAMP NOT NULL,
                prediction FLOAT NOT NULL
            );
        """)

        now = datetime.now().replace(minute=0, second=0, microsecond=0)
        for i in range(48):
            timestamp = now - timedelta(hours=i)
            cursor.execute(f"""
                INSERT INTO {table_name} VALUES (
                    '{timestamp}', 1.0, 1.0, 1.0, 1.0, 20.0, 1000.0, 15.0, 2.0, '{timestamp}', 1.0, 'N'
                );
            """)

            cursor.execute(f"""
                INSERT INTO {prediction_table_name} (reading_time, prediction) VALUES (
                    '{timestamp}', 1.0
                );
            """)
        conn.commit()
        conn.close()

        yield {
            "POSTGRES_USER": user,
            "POSTGRES_PASSWORD": password,
            "POSTGRES_HOST": host_and_port,
            "POSTGRES_DB": db,
            "TABLE_NAME": table_name,
            "PREDICTION_TABLE_NAME": prediction_table_name
        }


@pytest.fixture(autouse=True)
def prefct_test_fixture():
    with prefect_test_harness():
        yield


def test_daily_drift(postgres_container, monkeypatch, caplog):
    caplog.set_level("INFO")
    monkeypatch.setenv("POSTGRES_USER", postgres_container["POSTGRES_USER"])
    monkeypatch.setenv("POSTGRES_PASSWORD", postgres_container["POSTGRES_PASSWORD"])
    monkeypatch.setenv("POSTGRES_HOST", postgres_container["POSTGRES_HOST"])
    monkeypatch.setenv("POSTGRES_DB", postgres_container["POSTGRES_DB"])
    monkeypatch.setenv("TABLE_NAME", postgres_container["TABLE_NAME"])
    monkeypatch.setenv("PREDICTION_TABLE_NAME", postgres_container["PREDICTION_TABLE_NAME"])
    monkeypatch.setenv("REPORTS_DIR", "/tmp")  # Set a temporary directory for reports

    monkeypatch.setenv("SLACK_WEBHOOK_URL", "N/A")

    daily_drift()

    assert "ALl good" in caplog.messages


@patch('src.pipeline.drift_check.run_deployment')
def test_daily_drift_bad_performance(mock_run_deployment, postgres_container, monkeypatch, caplog):
    caplog.set_level("INFO")
    mock_run_deployment.return_value = None

    conn = configuration(monkeypatch, postgres_container)
    cursor = conn.cursor()
    # Change all predictions to 0.0 to simulate terrible model performance
    table_name = postgres_container["PREDICTION_TABLE_NAME"]
    cursor.execute(f"""
        UPDATE {table_name}
        SET prediction = 0.0
        WHERE reading_time IN (
            SELECT reading_time
            FROM {table_name}
            ORDER BY reading_time DESC
            LIMIT 24
        );
    """)
    conn.commit()
    conn.close()

    daily_drift()

    assert "Model degrading while data isn't drifting" in caplog.messages

    mock_run_deployment.assert_called_once_with(
        name="main_flow/automated-retrain-deployment",
        timeout=0
    )


def test_daily_drift_data_drift(postgres_container, monkeypatch, caplog):
    caplog.set_level("INFO")

    conn = configuration(monkeypatch, postgres_container)
    cursor = conn.cursor()

    table_name = postgres_container["TABLE_NAME"]
    cursor.execute(f"""
        UPDATE {table_name}
        SET so2 = 100.0 ,no2 = 100.0, co = 100.0, o3 = 100.0, temp = 100.0, pres = 100.0, dewp = 100.0, wspm = 100.0
        WHERE updated IN (
            SELECT updated
            FROM {table_name}
            ORDER BY updated DESC
            LIMIT 24
        );
    """)
    conn.commit()
    conn.close()

    daily_drift()

    assert "Data Drifted but model still performing good" in caplog.messages


def test_daily_drift_bad_model_and_data_drift(postgres_container, monkeypatch, caplog):
    caplog.set_level("INFO")

    conn = configuration(monkeypatch, postgres_container)
    cursor = conn.cursor()

    table_name = postgres_container["TABLE_NAME"]
    prediction_table_name = postgres_container["PREDICTION_TABLE_NAME"]
    cursor.execute(f"""
        UPDATE {table_name}
        SET so2 = 100.0 ,no2 = 100.0, co = 100.0, o3 = 100.0,
        temp = 100.0, pres = 100.0, dewp = 100.0, wspm = 100.0
        WHERE reading_time IN (
            SELECT reading_time
            FROM {table_name}
            ORDER BY reading_time DESC
            LIMIT 24
        );
    """)
    cursor.execute(f"""
        UPDATE {prediction_table_name}
        SET prediction = 0.0
        WHERE reading_time IN (
            SELECT reading_time
            FROM {table_name}
            ORDER BY reading_time DESC
            LIMIT 24
        );
    """)
    conn.commit()
    conn.close()

    daily_drift()

    assert "Check Sensors reads..." in caplog.messages
