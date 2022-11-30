import pickle

from flypipe import node
from flypipe.examples.pipeline.model.demo.config import PRODUCTION_RUN_ID, ARTIFACT_LOCATION
from flypipe.schema import Schema
from flypipe.examples.pipeline.model.demo.data import data
import mlflow


@node(
    type="pandas",
    description="Split train (70%) and test (30%) data",
    tags=["data", "split"],
    dependencies=[
        data.select(
            'sepal_length',
            'sepal_width',
            'petal_length',
            'petal_width',
        )
    ],
    output=Schema(
        data.output.get("sepal_length"),
        data.output.get("sepal_width"),
        data.output.get("petal_length"),
        data.output.get("petal_width"),
    ))
def scale(data, run_id=PRODUCTION_RUN_ID):
    X_cols = [
        'sepal_length',
        'sepal_width',
        'petal_length',
        'petal_width'
    ]

    with open(f'{ARTIFACT_LOCATION}{run_id}/model/scaler.pkl', 'rb') as fp:
        scaler = pickle.load(fp)
        data[X_cols] = scaler.transform(data[X_cols])

    return data
