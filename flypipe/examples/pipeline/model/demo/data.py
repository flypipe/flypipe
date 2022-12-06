from flypipe import node
from flypipe.examples.pipeline.dwh.raw.iris import iris
from flypipe.schema import Schema


@node(
    type="pandas",
    description="Data for model",
    tags=["data"],
    dependencies=[
        iris.select(
            "sepal_length", "sepal_width", "petal_length", "petal_width", "target"
        )
    ],
    output=Schema(
        iris.output.get("sepal_length"),
        iris.output.get("sepal_width"),
        iris.output.get("petal_length"),
        iris.output.get("petal_width"),
        iris.output.get("target"),
    ),
)
def data(iris):
    return iris
