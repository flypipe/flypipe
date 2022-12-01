from flypipe import node
from flypipe.examples.pipeline.model.demo.train.evaluate import evaluate
from flypipe.examples.pipeline.model.demo.predict.predict import predict


@node(
    type="pandas",
    description="Graph to train and predict Iris Data set",
    dependencies=[
        evaluate,
        predict
    ])
def graph(evaluate, predict):
    raise NotImplemented('Not supposed to run, only used to display the graph')
