SELECT 'CREATE DATABASE prefect_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'prefect_db')\gexec
SELECT 'CREATE DATABASE mlflow_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow_db')\gexec
SELECT 'CREATE DATABASE grafana_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'grafana_db')\gexec

-- ==========================================
-- PART 2: AIR POLLUTION DATA STRUCTURES
-- ==========================================

-- this tables will be created inside a database called "postgres" that because the compose-db.yaml the database name is postgres

-- Step 1: Create the table safely
CREATE TABLE IF NOT EXISTS air_pollution (
 id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY NOT NULL,
 reading_time TIMESTAMP NOT NULL,
 SO2 FLOAT NULL,
 NO2 FLOAT NULL,
 CO FLOAT NULL,
 O3 FLOAT NULL,
 TEMP FLOAT NULL,
 PRES FLOAT NULL,
 DEWP FLOAT NULL,
 RAIN FLOAT NULL,
 wd VARCHAR(255) NULL,
 WSPM FLOAT NULL,
 real_output FLOAT NULL,
 created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- step 2: Create a prediction table to join with the air_pollution table
CREATE TABLE IF NOT EXISTS air_pollution_predictions (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY NOT NULL,
    reading_time TIMESTAMP NOT NULL,
    prediction FLOAT NOT NULL
);

-- Step 2: Create a reusable function to update the timestamp
-- (Safe to run multiple times because of "OR REPLACE")
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Step 3: Attach this function to your table as a Trigger
-- (Drop the trigger first so it doesn't error out if it already exists)
DROP TRIGGER IF EXISTS update_air_pollution_modtime ON air_pollution;

CREATE TRIGGER update_air_pollution_modtime
BEFORE UPDATE ON air_pollution
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();
