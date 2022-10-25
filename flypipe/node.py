import logging
from typing import Mapping, List
from flypipe.dataframe.dataframe import DataFrame
from flypipe.exceptions import NodeTypeInvalidError
from flypipe.node_input import InputNode
from flypipe.node_result import NodeResult
from flypipe.node_run_context import NodeRunContext
from flypipe.node_type import NodeType
from flypipe.utils import DataFrameType, dataframe_type

logger = logging.getLogger(__name__)


class Node:
    node_type = NodeType.TRANSFORMATION
    TYPE_MAP = {
        'pyspark': DataFrameType.PYSPARK,
        'pandas': DataFrameType.PANDAS,
        'pandas_on_spark': DataFrameType.PANDAS_ON_SPARK,
    }

    def __init__(self,
                 function,
                 type: str,
                 description=None,
                 tags=None,
                 dependencies: List[InputNode] = None,
                 output=None,
                 spark_context=False):
        self.function = function
        try:
            self.type = self.TYPE_MAP[type]
        except KeyError:
            raise NodeTypeInvalidError(f'Invalid type {type}, expected one of {",".join(self.TYPE_MAP.keys())}')

        # TODO: enforce tags for now, later validation can be set as optional via environment variable
        self.description = description or "No description"

        # TODO: enforce tags for now, later validation can be set as optional via environment variable
        self.tags = tags or []

        self.input_nodes = dependencies or []

        self._provided_inputs = {}

        # TODO: enforce tags for now, later validation can be set as optional via environment variable
        self.output_schema = output

        self.spark_context = spark_context
        self.node_graph = None

    @property
    def __name__(self):
        """Return the name of the wrapped transformation rather than the name of the decorator object"""
        # TODO: replace with regex only a-z and 0-9 digits
        return self.varname.replace(".", "_")

    @property
    def varname(self):
        """Return the variable name of the wrapped transformation rather than the name of the decorator object"""
        return self.function.__name__

    @property
    def __doc__(self):
        """Return the docstring of the wrapped transformation rather than the docstring of the decorator object"""
        return self.function.__doc__

    @classmethod
    def get_class(cls, node_type):
        # I put the import here to avoid a circular import error
        from flypipe.pandas_on_spark_node import PandasOnSparkNode
        if node_type == 'pandas_on_spark':
            return PandasOnSparkNode
        else:
            return Node

    def _create_graph(self, pandas_on_spark_use_pandas=False):
        from flypipe.node_graph import NodeGraph
        self.node_graph = NodeGraph(self, pandas_on_spark_use_pandas=pandas_on_spark_use_pandas)
        self.node_graph.calculate_graph_run_status(self.__name__, self._provided_inputs)

    def select(self, *columns):
        # TODO- if self.output_schema is defined then we should ensure each of the columns is in it.
        # otherwise if self.output_schema is not defined then we won't know the ultimate output schema
        # so can't do any validation

        return InputNode(self, list(columns))

    def inputs(self, **kwargs):
        for k, v in kwargs.items():
            #TODO: apply same replacement as defined in method __name__
            self._provided_inputs[k.replace(".","_")] = v

        return self

    def clear_inputs(self):
        self._provided_inputs = {}
        return self

    def __call__(self, *args):
        return self.function(*args)

    def run(self, spark=None, parallel=True, pandas_on_spark_use_pandas=False):
        self._create_graph(pandas_on_spark_use_pandas)
        if parallel:
            raise NotImplementedError
        else:
            return self._run_sequential(spark)

    @property
    def input_dataframe_type(self):
        return self.type

    def get_node_inputs(self, outputs: Mapping[str, NodeResult]):
        inputs = {}
        for input_node in self.input_nodes:
            node_input_value = outputs[input_node.__name__].as_type(self.input_dataframe_type)
            inputs[input_node.get_alias()] = node_input_value.select_columns(*input_node.selected_columns).df
        return inputs

    def _run_sequential(self, spark=None):
        outputs = {k: NodeResult(spark, df, schema=None) for k, df in self._provided_inputs.items()}
        execution_graph = self.node_graph.copy()

        while not execution_graph.is_empty():
            runnable_nodes = execution_graph.pop_runnable_transformations()
            for runnable_node in runnable_nodes:
                if runnable_node.__name__ in outputs:
                    continue

                dependency_values = runnable_node.get_node_inputs(outputs)
                result = NodeResult(
                    spark,
                    runnable_node.process_transformation(spark, **dependency_values),
                    runnable_node.output_schema
                )
                output_columns = self.node_graph.get_node_output_columns(runnable_node.__name__)
                if output_columns:
                    result.select_columns(*output_columns)

                outputs[runnable_node.__name__] = result

        return outputs[self.__name__].as_type(self.type).df


    def process_transformation(self, spark, **inputs):
        # TODO: apply output validation + rename function to transformation, select only necessary columns specified in self.dependencies_selected_columns
        if self.spark_context:
            parameters = {'spark': spark, **inputs}
        else:
            parameters = inputs

        return self.function(**parameters)

    def plot(self):
        self.node_graph.plot()

    def html(self, width=-1, height=1000, pandas_on_spark_use_pandas=False):
        from flypipe.printer.graph_html import GraphHTML
        self._create_graph(pandas_on_spark_use_pandas)
        return GraphHTML(self.node_graph, width=width, height=height).html()


def node(type, *args, **kwargs):
    """
    Decorator factory that returns the given function wrapped inside a Node class
    """

    def decorator(func):
        kwargs['type'] = type
        return Node.get_class(type)(func, *args, **kwargs)

    return decorator
