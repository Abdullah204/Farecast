import functions
import os
import pandas as pd


def execute():
    engine = functions.connect_to_db()
    if not os.path.exists("data/green_trip_data_18-8_clean.csv"):
        df, lookup_table = functions.call_all_dataset_related_functions()
    else:
        df = pd.read_csv("data/green_trip_data_18-8_clean.csv")
        lookup_table = pd.read_csv("data/lookup_table_green_taxis.csv")

    functions.save_to_csv(df, lookup_table)
    functions.save_to_parquet(df)
    functions.save_cleaned_and_lookup(df, lookup_table, engine)


execute()
