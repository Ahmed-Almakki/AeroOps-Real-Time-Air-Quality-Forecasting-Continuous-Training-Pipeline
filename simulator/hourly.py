import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
engine = create_engine(
    f"postgresql+psycopg://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@localhost:5433/{os.getenv('POSTGRES_DB')}"
)
file_name = "PRSA_Data_Wanshouxigong_20130301-20170228.csv"


def read_excel(path):
    df = pd.read_csv(path)
    popout_list = ["No", "year", "day", "month", "hour", "station", "PM10"]
    df = df.drop(columns=popout_list)
    df.columns = df.columns.str.lower()
    df["prediction"] = df["pm2.5"].shift(-1)
    df = df.dropna()
    df = df.rename(columns={"pm2.5": "real_output"})
    test = df.tail(20)
    test.to_sql(name=os.getenv("TABLE_NAME"), con=engine, if_exists="append", index=False)


file = os.path.join("data", file_name)

read_excel(file)
