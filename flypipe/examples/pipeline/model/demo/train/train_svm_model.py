import mlflow
from mlflow.models.signature import infer_signature
from sklearn import svm

from flypipe import node
from flypipe.examples.pipeline.model.demo.train.fit_scale import fit_scale
from flypipe.schema import Schema, Column
from flypipe.schema.types import String, Integer


@node(
    type="pandas",
    description="Model training using SVM",
    tags=["model_training", "svm"],
    dependencies=[
        fit_scale.select(
            "data_type",
            "sepal_length",
            "sepal_width",
            "petal_length",
            "petal_width",
            "target",
        ).alias("df")
    ],
    output=Schema(
        Column("data_type", String(), "train (70%), test (30%)"),
        fit_scale.output.get("sepal_length"),
        fit_scale.output.get("sepal_width"),
        fit_scale.output.get("petal_length"),
        fit_scale.output.get("petal_width"),
        fit_scale.output.get("target"),
        Column("prediction", Integer(), "prediction"),
    ),
)
def train_svm_model(df):
    X_cols = ["sepal_length", "sepal_width", "petal_length", "petal_width"]

    X_train = df[df["data_type"] == "train"]
    y_train = X_train["target"]
    X_train = X_train[X_cols]

    clf = svm.SVC().fit(X_train, y_train)

    if mlflow.active_run():
        signature = infer_signature(X_train, y_train)
        mlflow.sklearn.log_model(
            clf, "model", signature=signature, input_example=X_train.head(5)
        )

    df["prediction"] = clf.predict(df[X_cols])
    return df
