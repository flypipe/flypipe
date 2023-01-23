import os
import pickle

import mlflow
from sklearn.preprocessing import StandardScaler

from flypipe import node
from flypipe.examples.pipeline.model.demo.config import config
from flypipe.examples.pipeline.model.demo.train.split import split
from flypipe.schema import Schema, Column
from flypipe.schema.types import String


@node(
    type="pandas",
    description="Fits a standard scaler",
    tags=["model_training", "scaler"],
    dependencies=[
        config.select("artifact_location"),
        split.select(
            "data_type",
            "sepal_length",
            "sepal_width",
            "petal_length",
            "petal_width",
            "target",
        ),
    ],
    output=Schema(
        Column("data_type", String(), "train (70%), test (30%)"),
        split.output.get("sepal_length"),
        split.output.get("sepal_width"),
        split.output.get("petal_length"),
        split.output.get("petal_width"),
        split.output.get("target"),
    ),
)
def fit_scale(config, split):
    X_cols = ["sepal_length", "sepal_width", "petal_length", "petal_width"]

    scaler = StandardScaler()
    scaler = scaler.fit(split[split["data_type"] == "train"][X_cols])

    if mlflow.active_run():
        artifact_location = config.loc[0, "artifact_location"]
        artifact_location = (
            f"{artifact_location}/{mlflow.active_run().info.run_id}/model"
        )
        if not os.path.exists(artifact_location):
            os.makedirs(artifact_location, exist_ok=True)

        with open(os.path.join(artifact_location, "scaler.pkl"), "wb") as f:
            pickle.dump(scaler, f)

    split[X_cols] = scaler.transform(split[X_cols])

    return split
