import pandas as pd
from sklearn import datasets

from flypipe import node
from flypipe.schema import Schema, Column
from flypipe.schema.types import Float, Integer


@node(
    type="pandas",
    description="Load Iris dataset",
    tags=["data"],
    output=Schema(
        Column("sepal_length", Float(), "sepal length"),
        Column("sepal_width", Float(), "sepal width"),
        Column("petal_length", Float(), "petal length"),
        Column("petal_width", Float(), "petal width"),
        Column("target", Integer(), "0: Setosa, 1: Versicolour, and 2: Virginica"),
    ),
)
def iris():
    iris = datasets.load_iris()
    df = pd.DataFrame(
        data={
            "sepal_length": iris.data[:, 0],
            "sepal_width": iris.data[:, 1],
            "petal_length": iris.data[:, 2],
            "petal_width": iris.data[:, 3],
            "target": iris.target,
        }
    )

    return df
