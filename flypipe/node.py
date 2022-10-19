import logging

from flypipe.dataframe_wrapper import DataframeWrapper
from flypipe.node_graph import NodeGraph
from flypipe.node_input import InputNode
from flypipe.node_type import NodeType
from flypipe.printer.graph_html import GraphHTML
from flypipe.transformation import Transformation
from flypipe.utils import DataFrameType, dataframe_type

logger = logging.getLogger(__name__)


class Node(Transformation):
    node_type = NodeType.TRANSFORMATION

    def _create_graph(self):
        self.node_graph = NodeGraph(self)
        self.node_graph.calculate_graph_run_status(self.__name__, self._provided_inputs)

    def select(self, *columns):
        # TODO- if self.output_schema is defined then we should ensure each of the columns is in it.
        # otherwise if self.output_schema is not defined then we won't know the ultimate output schema so can't do any validation

        # TODO- DON'T DO THIS HERE THIS IS CALLED AT DEFINITION TIME
        for column in columns:
            self.requested_output_columns.append(column)

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

    def _run_sequential(self, spark=None):
        outputs = {k: DataframeWrapper(spark, df, schema=None) for k, df in self._provided_inputs.items()}
        node_graph = self.node_graph.copy()

        while not node_graph.is_empty():
            transformations = node_graph.pop_runnable_transformations()
            for transformation in transformations:
                if transformation.__name__ in outputs:
                    continue

                dependency_values = {}
                for node_input in transformation.dependencies:
                    node_input_value = outputs[node_input.__name__].as_type(
                        transformation.type)

                    # Only select the columns that were requested in the dependency definition
                    if transformation.type in (DataFrameType.PANDAS, DataFrameType.PANDAS_ON_SPARK):
                        node_input_value = node_input_value[node_input.selected_columns]
                    elif transformation.type == DataFrameType.PYSPARK:
                        node_input_value = node_input_value.select(node_input.selected_columns)

                    dependency_values[node_input.__name__] = node_input_value

                result = self.process_transformation(spark, transformation, **dependency_values)
                # TODO- once output schema is implemented, we should only use output_columns if the output schema
                # isn't provided
                result_type = dataframe_type(result)
                if result_type in (DataFrameType.PANDAS, DataFrameType.PANDAS_ON_SPARK):
                    result = result[transformation.output_columns]
                elif result_type == DataFrameType.PYSPARK:
                    result = result.select(transformation.output_columns)

                output = DataframeWrapper(spark, result, transformation.output_schema)

                outputs[transformation.__name__] = output

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


def node(*args, **kwargs):
    """
    Decorator factory that returns the given function wrapped inside a Node class
    """

    def decorator(func):
        return Node(func, *args, **kwargs)

    return decorator