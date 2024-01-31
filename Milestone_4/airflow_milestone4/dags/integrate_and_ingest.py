import pandas as pd
import psycopg2
from sqlalchemy import create_engine


def integrate_and_ingest():
    df = pd.read_csv("./data/green_tripdata_2018-08_clean_with_gps.csv")
    lookup_table = pd.read_csv("./data/lookup_table_green_taxis.csv")
    engine = connect_to_db()
    try:
        df.to_sql("M4_green_taxis_08_2018", engine, if_exists="fail", index=False)
        lookup_table.to_sql("lookup_table", engine, if_exists="fail", index=False)
    except ValueError as vx:
        print("Tables already exists")


def connect_to_db():
    engine = create_engine("postgresql://root:root@pgdatabase:5432/nyc_etl")
    engine.connect()
    return engine
