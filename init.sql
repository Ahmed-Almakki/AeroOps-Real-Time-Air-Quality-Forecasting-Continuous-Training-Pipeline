CREATE TABLE IF NOT EXISTS air_pollution (
 id INT PRIMARY KEY NOT NULL,
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
 targist FLOAT NULL,
 prediction FLOAT  NULL,
 created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Step 1: Create a reusable function that updates the timestamp
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Step 2: Attach this function to your table as a Trigger
CREATE TRIGGER update_air_pollution_modtime
BEFORE UPDATE ON air_pollution
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();