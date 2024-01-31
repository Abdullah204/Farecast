import m2_functions
import os
import pandas as pd


def clean_and_transform():
    if not os.path.exists("./data/green_tripdata_2018-08_clean.csv"):
        df = pd.read_csv("./data/green_tripdata_2018-08.csv")
        df, lookup_table = m2_functions.call_all_dataset_related_functions(df)
        df.to_csv("./data/green_tripdata_2018-08_clean.csv", index=False)
        lookup_table.to_csv("./data/lookup_table_green_taxis.csv", index=False)
