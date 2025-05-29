from datetime import datetime


def preprocess_config(df):
    return df[df["datetime_created"] == datetime(2025, 1, 2)]
