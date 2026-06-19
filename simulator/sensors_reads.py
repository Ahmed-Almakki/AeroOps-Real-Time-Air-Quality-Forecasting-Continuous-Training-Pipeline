from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session


load_dotenv()
engine = create_engine(f"postgresql+psycopg://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@localhost:5433/{os.getenv('POSTGRES_DB')}")
# print("start inserting...\n", engine)
# with Session(engine) as session:
#     print("before query inside session")
#     query = f'INSERT INTO {os.getenv("TABLE_NAME")} ("so2", "no2", "co", "o3", "temp", "pres", "dewp", "rain", "wd", "wspm") '
#     print("befor loop")
#     for i in range(2, 4):
#         final_query = query + f"VALUES ({i * 1}, {i * 2}, {i * 3}, {i * 4}, {i * 5}, {i * 6}, {i * 7}, {i * 8}, {i * 9}, {i * 10});"
#         print(f"the query is:\n {final_query}")
#         query_text = text(final_query)
#         print(f"the qury final form: \n\n\n{query_text}")
#         session.execute(query_text)
#         print("after the sesssion")
#     session.commit()
# print("Done")

file_name = "PRSA_Data_Aotizhongxin_20130301-20170228.csv"


def read_excel(path):
    df = pd.read_csv(path)
    popout_list = ["No", "year", "day", "month", "hour", "station", "PM10"]
    df = df.drop(columns=popout_list)
    input_features = df.columns.tolist()
    df.columns = df.columns.str.lower()
    df["prediction"] = df["pm2.5"].shift(-1)
    df = df.dropna()
    df = df.rename(columns={"pm2.5": "real_output"})
    df.to_sql(
        name=os.getenv("TABLE_NAME"),
        con=engine,
        if_exists="append",
        index=False
    )
   

file = os.path.join("data", file_name)

read_excel(file)