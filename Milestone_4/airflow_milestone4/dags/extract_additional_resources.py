import pandas as pd
import os
import m2_functions


def extract_additional_resources():
    if not os.path.exists("./data/green_tripdata_2018-08_clean_with_gps.csv"):
        df = pd.read_csv("./data/green_tripdata_2018-08_clean.csv")
        df = m2_functions.add_gps(df)
        df.to_csv(
            "./data/green_tripdata_2018-08_clean_with_gps.csv",
            index=False,
        )
