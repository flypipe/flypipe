import pandas as pd
from sklearn.model_selection import train_test_split

from flypipe import node
from flypipe.examples.pipeline.model.demo.data import data
from flypipe.schema import Schema, Column
from flypipe.schema.types import String


@node(
    type="pandas",
    description="Split train (70%) and test (30%) data",
    tags=["model_training"],
    dependencies=[
        data.select(
            "sepal_length",
            "sepal_width",
            "petal_length",
            "petal_width",
            "target",
        )
    ],
    output=Schema(
        Column("data_type", String(), "train (70%), test (30%)"),
        data.output.get("sepal_length"),
        data.output.get("sepal_width"),
        data.output.get("petal_length"),
        data.output.get("petal_width"),
        data.output.get("target"),
    ),
)
def split(data):
    data["data_type"] = "train"

    X_cols = ["sepal_length", "sepal_width", "petal_length", "petal_width"]
    y_col = "target"

    X_train, X_test, y_train, y_test = train_test_split(
        data[X_cols], data[y_col], test_size=0.3, random_state=1
    )

    X_train["data_type"] = "train"
    X_train["target"] = y_train

    X_test["data_type"] = "test"
    X_test["target"] = y_test

    data = pd.concat([X_train, X_test])
    return data
