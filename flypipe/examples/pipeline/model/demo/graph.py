from flypipe import node
from flypipe.examples.pipeline.model.demo.predict.predict import predict
from flypipe.examples.pipeline.model.demo.train.evaluate import evaluate


@node(
    type="pandas",
    description="Graph to train and predict Iris Data set",
    dependencies=[evaluate, predict],
)
def graph(evaluate, predict):
    raise NotImplementedError("Not supposed to run, only used to display the graph")
