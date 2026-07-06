import psycopg2
from testcontainers.postgres import PostgresContainer
import pytest
from datetime import datetime, timedelta
from prefect.testing.utilities import prefect_test_harness

from src.pipeline.drift_check import daily_drift


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
                prediction FLOAT, real_output FLOAT, wd VARCHAR
            );
        """)

        now = datetime.now()
        for i in range(48):
            timestamp = now - timedelta(hours=i)
            cursor.execute(f"""
                INSERT INTO {table_name} VALUES (
                    '{timestamp}', 1.0, 1.0, 1.0, 1.0, 20.0, 1000.0, 15.0, 2.0, 1.0, 1.0, 'N'
                );
            """)
        conn.commit()
        conn.close()

        yield {
            "POSTGRES_USER": user,
            "POSTGRES_PASSWORD": password,
            "POSTGRES_HOST": host_and_port,
            "POSTGRES_DB": db,
            "TABLE_NAME": table_name
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
    monkeypatch.setenv("REPORTS_DIR", "/tmp")  # Set a temporary directory for reports

    monkeypatch.setenv("SLACK_WEBHOOK_URL", "N/A")

    daily_drift()

    assert "ALl good" in caplog.messages
