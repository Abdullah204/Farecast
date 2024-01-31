import plotly.express as px
from dash import Dash, dcc, html, Input, Output
import pandas as pd


def scatter(df, attribute1, attribute2):
    fig = px.scatter(df, x=attribute1, y=attribute2)
    fig.update_layout(xaxis_title=attribute1, yaxis_title=attribute2)
    return fig


def pups_per_hour(df):
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
    df["pu_hour"] = df["pickup_datetime"].dt.hour
    pph = df["pu_hour"].value_counts().sort_index()
    df.drop(columns=["pu_hour"], inplace=True)
    fig = px.bar(
        x=pph.index,
        y=pph,
        labels={"x": "Hour in Day", "y": "Pickup Count"},
    )
    return fig


def avg_fare_per_payment_type():
    df = pd.read_csv("./data/green_tripdata_2018-08_clean_with_gps.csv")
    avg = df.groupby("payment_type")["fare_amount"].mean()
    fig = px.bar(
        x=avg.index,
        y=avg,
        labels={"x": "Payment Type", "y": "Average Fare Amount"},
    )
    return fig


def payment_type_distribution():
    df = pd.read_csv("./data/green_tripdata_2018-08_clean_with_gps.csv")
    dist = df["payment_type"].value_counts()
    # i want to draw it in bar format
    fig = px.bar(
        x=dist.index,
        y=dist,
        labels={"x": "Payment Type", "y": "Number of Trips"},
    )
    return fig


def create_dashboard():
    df = pd.read_csv("./data/green_tripdata_2018-08_clean_with_gps.csv")

    app = Dash()
    app.layout = html.Div(
        children=[
            html.H1("Abdullah Ahmad Fouad 49-2554 MET", style={"text-align": "center"}),
            html.H2("fare amounts Distribution", style={"text-align": "center"}),
            dcc.Graph(
                id="fare-amounts-histogram",
                figure=px.histogram(df, x="fare_amount", nbins=10),
            ),
            html.H2("Trip Distance Distribution", style={"text-align": "center"}),
            dcc.Graph(
                id="trip-distance-histogram",
                figure=px.histogram(df, x="trip_distance", nbins=10),
            ),
            html.H2(
                "trip distance and fare amount scatter",
                style={"text-align": "center"},
            ),
            dcc.Graph(
                id="distance-fare-scatter",
                figure=scatter(df, "trip_distance", "fare_amount"),
            ),
            html.H2(
                "total amount and tip amoint scatter",
                style={"text-align": "center"},
            ),
            dcc.Graph(
                id="amount-tip-scatter",
                figure=scatter(df, "total_amount", "tip_amount"),
            ),
            html.H2("Distribution of Passenger Count ", style={"text-align": "center"}),
            dcc.Graph(
                id="passenger-count-histogram",
                figure=px.histogram(df, x="passenger_count", nbins=10),
            ),
            html.H2(
                "Hourly Pickup count",
                style={"text-align": "center"},
            ),
            dcc.Graph(id="pickup-counts-by-hour", figure=pups_per_hour(df)),
            html.H2("Payment Type Distribution", style={"text-align": "center"}),
            dcc.Graph(
                id="payment-type-distribution", figure=payment_type_distribution()
            ),
            html.H2(
                "Average Fare Amount by Payment Type", style={"text-align": "center"}
            ),
            dcc.Graph(
                id="average-fare-amount-by-payment-type",
                figure=avg_fare_per_payment_type(),
            ),
        ]
    )
    app.run_server(host="0.0.0.0", debug=False)
