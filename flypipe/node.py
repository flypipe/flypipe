import logging
from typing import Mapping
from flypipe.dataframe.dataframe import DataFrame
from flypipe.node_graph import NodeGraph
from flypipe.node_input import InputNode
from flypipe.node_result import NodeResult
from flypipe.node_type import NodeType
from flypipe.printer.graph_html import GraphHTML
from flypipe.transformation import Transformation
from flypipe.utils import DataFrameType, dataframe_type

logger = logging.getLogger(__name__)


class Node(Transformation):
    node_type = NodeType.TRANSFORMATION

    @classmethod
    def get_class(cls, node_type):
        # I put the import here to avoid a circular import error
        from flypipe.pandas_on_spark_node import PandasOnSparkNode
        if node_type == 'pandas_on_spark':
            return PandasOnSparkNode
        else:
            return Node

    def _create_graph(self):
        self.node_graph = NodeGraph(self)
        self.node_graph.calculate_graph_run_status(self.__name__, self._provided_inputs)

    def select(self, *columns):
        # TODO- if self.output_schema is defined then we should ensure each of the columns is in it.
        # otherwise if self.output_schema is not defined then we won't know the ultimate output schema so can't do any validation

        return InputNode(self, list(columns))

    def inputs(self, **kwargs):
        for k, v in kwargs.items():
            self._provided_inputs[k.replace(".","_")] = v

        return self

    def clear_inputs(self):
        self._provided_inputs = {}
        return self

    def __call__(self, *args):
        return self.function(*args)

    def run(self, spark=None, parallel=True):
        self._create_graph()
        if parallel:
            raise NotImplementedError
        else:
            return self._run_sequential(spark)

    @property
    def input_dataframe_type(self):
        return self.type

    def get_node_inputs(self, outputs: Mapping[str, NodeResult]):
        inputs = {}
        for node_input in self.dependencies:
            node_input_value = outputs[node_input.__name__].as_type(self.input_dataframe_type)
            # TODO: problem- how will the node flag translate to converting all inputs to a pandas on spark node to pandas?

            # TODO: how do we cast the type and also filter the columns?
            # Only select the columns that were requested in the dependency definition

            inputs[node_input.__name__] = node_input_value.select_columns(*node_input.selected_columns)
        return inputs

    def _run_sequential(self, spark=None):
        outputs = {k: NodeResult(spark, df, schema=None) for k, df in self._provided_inputs.items()}
        execution_graph = self.node_graph.copy()

        while not execution_graph.is_empty():
            transformations = execution_graph.pop_runnable_transformations()
            for transformation in transformations:
                if transformation.__name__ in outputs:
                    continue

                dependency_values = transformation.get_node_inputs(outputs)
                result = NodeResult(
                    spark,
                    self.process_transformation(spark, transformation, **dependency_values),
                    transformation.output_schema
                )
                output_columns = self.node_graph.get_node_output_columns(transformation.__name__)
                if output_columns:
                    result.select_columns(*output_columns)

                outputs[transformation.__name__] = result

        return outputs[self.__name__].as_type(self.type)


    def process_transformation(self, spark, transformation: Transformation, **inputs):
        # TODO: apply output validation + rename function to transformation, select only necessary columns specified in self.dependencies_selected_columns
        if transformation.spark_context:
            parameters = {'spark': spark, **inputs}
        else:
            parameters = inputs

        return transformation.function(**parameters)

    def plot(self):
        self.node_graph.plot()

    def html(self, width=-1, height=1000):
        self._create_graph()
        return GraphHTML(self.node_graph, width=width, height=height).html()


def node(type, *args, **kwargs):
    """
    Decorator factory that returns the given function wrapped inside a Node class
    """

    def decorator(func):
        kwargs['type'] = type
        return Node.get_class(type)(func, *args, **kwargs)

    return decorator