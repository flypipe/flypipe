import mlflow

from flypipe import node
from flypipe.examples.pipeline.model.demo.config import config
from flypipe.examples.pipeline.model.demo.predict.scale import scale
from flypipe.schema import Schema, Column
from flypipe.schema.types import Integer


@node(
    type="pandas",
    tags=["prediction"],
    dependencies=[
        config.select("production_run_id"),
        scale.select(
            "sepal_length", "sepal_width", "petal_length", "petal_width"
        ).alias("df"),
    ],
    output=Schema(
        Column("prediction", Integer(), "prediction"),
    ),
)
def predict(config, df):
    """
    Split train (70%) and test (30%) data
    """
    run_id = config.loc[0, "production_run_id"]
    model_path = f"runs:/{run_id}/model"
    loaded_model = mlflow.pyfunc.load_model(model_path)

    df["prediction"] = loaded_model.predict(df)
    return df
