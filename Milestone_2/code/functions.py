import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
from geopy.geocoders import GoogleV3
from sklearn.preprocessing import StandardScaler
import numpy as np
from sklearn.preprocessing import LabelEncoder

api_key = "AIzaSyBXV_Q4_CWvV7btH9drTwc3BYRoj2GwozQ"


def connect_to_db():
    engine = create_engine(
        "postgresql://abdullahahmadfouad:password123@db:5432/green_taxi_8_18_postgres"
    )
    engine.connect()
    return engine


def cleaned_data_exists():
    return os.path.exists("data/green_trip_data_18-8_clean.csv")


def save_table(df, engine, table_name):
    df.to_sql(table_name, engine, if_exists="append", index=False)


def save_cleaned_and_lookup(cleaned, lookup, engine):
    save_table(cleaned, engine, "green_taxi_8_18")
    save_table(lookup, engine, "lookup_green_taxi_8_18")


def call_all_dataset_related_functions():
    df = pd.read_csv("data/green_tripdata_2018-08.csv")
    lookup_table = pd.DataFrame()
    clean_column_names(df)
    negative_features = [
        "fare_amount",
        "total_amount",
        "tolls_amount",
        "tip_amount",
        "mta_tax",
        "extra",
        "improvement_surcharge",
    ]
    absolutize_features(df, negative_features)
    features_to_zero = ["ehail_fee", "extra", "congestion_surcharge"]
    replace_null_with_zeroes(df, features_to_zero)
    median_impute(df, "passenger_count")
    mv_impute(df, "store_and_fwd", "trip_type", "N", "Y", "Dispatch")
    set_payment_credit_when_tip(df)
    unify_unknowns_payment_type(df)
    typecast_datetime(df)
    add_week_number_and_date_range(df)
    generate_GPS(df)
    map_location_to_gps(
        df,
        [
            ["pu_latitude", "pu_location", "latitude"],
            ["pu_longitude", "pu_location", "longitude"],
            ["do_latitude", "do_location", "latitude"],
            ["do_longitude", "do_location", "longitude"],
        ],
    )
    positive_amount_outliers = [
        "tolls_amount",
        "tip_amount",
        "fare_amount",
        "total_amount",
    ]
    handle_positive_outliers(df, positive_amount_outliers)
    floor_cap_impute(df, "extra", 0.05, 0.95)
    label_features = ["trip_type", "payment_type", "rate_type"]
    lookup_table = label_encode(df, label_features, lookup_table=lookup_table)
    lookup_table = add_arbitrary_imputation_to_lookup_table(lookup_table)
    add_three_features(df)
    return (df, lookup_table)


def clean_column_names(df):
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(" ", "_")
    df.rename(
        columns={
            "lpep_pickup_datetime": "pickup_datetime",
            "lpep_dropoff_datetime": "dropoff_datetime",
            "store_and_fwd_flag": "store_and_fwd",
        },
        inplace=True,
    )


def absolutize_features(df, feature_names):
    for feature in feature_names:
        df[feature] = np.abs(df[feature])


def replace_null_with_zeroes(df, features):
    for feat in features:
        df[feat].fillna(0, inplace=True)


def median_impute(df, feat_name):
    median_value = df[feat_name].median()
    df[feat_name].fillna(median_value, inplace=True)


def mv_impute(df, feat1, feat2, main_val, other_val, condition_val):
    df[feat1] = df[feat1].fillna(main_val).where(df[feat2] == condition_val, other_val)


def set_payment_credit_when_tip(df):
    df["payment_type"] = df.apply(
        lambda row: "Credit card" if row["tip_amount"] > 0 else row["payment_type"],
        axis=1,
    )


def unify_unknowns_payment_type(df):
    df["payment_type"].fillna("Unknown", inplace=True)
    df["payment_type"] = df["payment_type"].replace("Uknown", "Unknown")


def typecast_datetime(df_copy):
    df_copy["pickup_datetime"] = pd.to_datetime(df_copy["pickup_datetime"])
    df_copy["dropoff_datetime"] = pd.to_datetime(df_copy["dropoff_datetime"])


def add_week_number_and_date_range(df):
    df["week_number"] = df["pickup_datetime"].dt.isocalendar().week
    df["date_range"] = df["pickup_datetime"].dt.to_period("W").astype(str)


def getGeolocator():
    return GoogleV3(api_key=api_key)


def getGPS(city_name):
    geolocator = getGeolocator()
    gps_loc = geolocator.geocode(city_name)
    try:
        gps_loc = geolocator.geocode(city_name)
        return (gps_loc.latitude, gps_loc.longitude)
    except:
        try:
            gps_loc = geolocator.geocode(city_name.split(",")[0])
            return (gps_loc.latitude, gps_loc.longitude)
        except:
            return (None, None)


def generate_GPS(df):
    path = "data/locations.csv"
    if os.path.exists(path):
        return pd.read_csv(path)
    unique_locations = pd.concat(
        [df["pu_location"], df["do_location"]], axis=0
    ).unique()
    mymap = pd.DataFrame(
        map(lambda x: (x, *getGPS(x)), unique_locations),
        columns=["location", "latitude", "longitude"],
    )
    mymap.to_csv(path)
    return mymap


def map_location_to_gps(df, params):
    location_map = generate_GPS(df)
    for param in params:
        df[param[0]] = df[param[1]].map(location_map.set_index("location")[param[2]])


def nonzero_mean_imputation(df, feature_name):
    rides_without_tolls = df[df[feature_name] > 0][feature_name]
    cutoff_pos = rides_without_tolls.mean() + rides_without_tolls.std() * 3
    cutoff_neg = rides_without_tolls.mean() - rides_without_tolls.std() * 3
    condition_series = (df[feature_name] > cutoff_pos) | (df[feature_name] < cutoff_neg)
    df[feature_name] = np.where(
        condition_series, rides_without_tolls.mean(), df[feature_name]
    )


def get_imputed_tolls_ride(df, feature_name):
    return df[df[feature_name] > 0][feature_name]


def view_non_zero_imputed_skew(df, feature_name):
    return get_imputed_tolls_ride(df, feature_name).skew()


def handle_positive_outliers(df, positive_amount_outliers):
    for feat in positive_amount_outliers:
        nonzero_mean_imputation(df, feat)


def get_floor_and_cap(df, feature, floor, cap):
    fvalue = df[feature].quantile(floor)
    cvalue = df[feature].quantile(cap)
    return fvalue, cvalue


def floor_cap_impute(df, feature, floor, cap):
    fvalue, cvalue = get_floor_and_cap(df, feature, floor, cap)
    df[feature] = np.where(df[feature] < fvalue, fvalue, df[feature])
    df[feature] = np.where(df[feature] > cvalue, cvalue, df[feature])


def add_mapping_to_lookup_table(mapping, feature_name, lookup_table):
    mapping_df = pd.DataFrame(list(mapping.items()), columns=["old_value", "new_value"])
    mapping_df["feature_name"] = [feature_name] * len(mapping_df)
    mapping_df = mapping_df[["feature_name", "old_value", "new_value"]]
    return pd.concat([lookup_table, mapping_df])


def label_encode(df, features, lookup_table):
    for feat in features:
        label_encoder = LabelEncoder()
        label_encoder.fit(df[feat])
        le_name_mapping = dict(
            zip(label_encoder.classes_, label_encoder.transform(label_encoder.classes_))
        )
        lookup_table = add_mapping_to_lookup_table(le_name_mapping, feat, lookup_table)
        df[feat] = label_encoder.transform(df[feat])
    return lookup_table


def add_three_features(df):
    df["trip_duration"] = (
        (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds() / 60
    ).astype(int)
    df["is_morning_trip"] = (df["pickup_datetime"].dt.hour < 12).astype(int)
    df["is_weekend_trip"] = (df["pickup_datetime"].dt.dayofweek >= 5).astype(int)


def standardize_features(feature_names, df):
    return pd.DataFrame(
        StandardScaler().fit_transform(df[feature_names]), columns=feature_names
    )


def add_arbitrary_imputation_to_lookup_table(lookup_table):
    data = {
        "feature_name": [
            "passenger_count",
            "extra",
            "congestion_surcharge",
            "ehaul_fee",
        ],
        "old_value": [
            111,
            "Null",
            "Null",
            "Null",
        ],
        "new_value": [1, 0, 0, 0],
    }
    return pd.concat([lookup_table, pd.DataFrame(data)])


def save_to_csv(df, lookup_table):
    df.to_csv("./data/green_trip_data_18-8_clean.csv", index=False)
    lookup_table.to_csv("./data/lookup_table_green_taxis.csv", index=False)


def save_to_parquet(df):
    df.to_parquet(
        "data/green_trip_data_18-8_clean.parquet",
        engine="pyarrow",
        compression="snappy",
    )
