import pickle

from flypipe import node
from flypipe.examples.pipeline.model.demo.config import config
from flypipe.examples.pipeline.model.demo.data import data
from flypipe.schema import Schema


@node(
    type="pandas",
    description="Split train (70%) and test (30%) data",
    tags=["prediction"],
    dependencies=[
        config.select("artifact_location", "production_run_id"),
        data.select(
            "sepal_length",
            "sepal_width",
            "petal_length",
            "petal_width",
        ),
    ],
    output=Schema(
        data.output.get("sepal_length"),
        data.output.get("sepal_width"),
        data.output.get("petal_length"),
        data.output.get("petal_width"),
    ),
)
def scale(config, data):
    X_cols = ["sepal_length", "sepal_width", "petal_length", "petal_width"]
    artifact_location = config.loc[0, "artifact_location"]
    run_id = config.loc[0, "production_run_id"]
    with open(f"{artifact_location}{run_id}/model/scaler.pkl", "rb") as fp:
        scaler = pickle.load(fp)
        data[X_cols] = scaler.transform(data[X_cols])

    return data
