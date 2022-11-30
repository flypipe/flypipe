import os
import pickle
import mlflow

from flypipe import node
from flypipe.schema import Schema, Column
from flypipe.schema.types import String
from sklearn.preprocessing import StandardScaler

from flypipe.examples.pipeline.model.demo.train.split import split


@node(
    type="pandas",
    description="Fits a standard scaler",
    tags=["data", "train", "scaler"],
    dependencies=[
        split.select(
            'data_type',
            'sepal_length',
            'sepal_width',
            'petal_length',
            'petal_width',
            'target',
        )
    ],
    output=Schema(
        Column('data_type', String(), 'train (70%), test (30%)'),
        split.output.get("sepal_length"),
        split.output.get("sepal_width"),
        split.output.get("petal_length"),
        split.output.get("petal_width"),
        split.output.get("target"),
    ))
def fit_scale(split):
    X_cols = [
        'sepal_length',
        'sepal_width',
        'petal_length',
        'petal_width'
    ]

    scaler = StandardScaler()
    scaler = scaler.fit(split[split['data_type'] == 'train'][X_cols])

    if mlflow.active_run():
        artifact_path = f"./artifacts/{mlflow.active_run().info.run_id}/model"
        if not os.path.exists(artifact_path):
            os.makedirs(artifact_path, exist_ok=True)

        pickle.dump(scaler, open(os.path.join(artifact_path, 'scaler.pkl'), 'wb'))

    split[X_cols] = scaler.transform(split[X_cols])

    return split
