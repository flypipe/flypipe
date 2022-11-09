import re
import sys
from typing import Mapping, List

from flypipe.config import get_config, RunMode
from flypipe.exceptions import NodeTypeInvalidError
from flypipe.node_input import InputNode
from flypipe.node_result import NodeResult
from flypipe.node_type import NodeType
from flypipe.schema import Schema, Column
from flypipe.schema.types import Unknown
from flypipe.utils import DataFrameType


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
        self._key = None
        self.function = function
        try:
            self.type = self.TYPE_MAP[type]
        except KeyError:
            raise NodeTypeInvalidError(f'Invalid type {type}, expected one of {",".join(self.TYPE_MAP.keys())}')

        if not description and get_config('require_node_description'):
            raise ValueError(
                f'Node description configured as mandatory but no description provided for node {self.__name__}')
        self.description = description or "No description"

        # TODO: enforce tags for now, later validation can be set as optional via environment variable
        self.tags = [self.type.value, self.node_type.value]
        if tags:
            self.tags.extend(tags)

        self.input_nodes = dependencies or []

        self._provided_inputs = {}

        # TODO: enforce tags for now, later validation can be set as optional via environment variable
        self.output_schema = output

        self.spark_context = spark_context
        self.node_graph = None

    @property
    def __name__(self):
        return self.function.__name__

    @property
    def __class__(self):
        return self.function.__class__

    @property
    def __package__(self):
        # When running a pipeline of node declared in the same
        # notebook, it throws an error as it not finds __package
        # in that case, returns nothing
        if hasattr(sys.modules[self.function.__module__], '__package'):
            return sys.modules[self.function.__module__].__package__

    @property
    def __file__(self):
        # When running a pipeline of node declared in the same
        # notebook, it throws an error as it not finds __file__
        # in that case, returns nothing
        if hasattr(sys.modules[self.function.__module__], '__file__'):
            return sys.modules[self.function.__module__].__file__

    @property
    def __module__(self):
        return self.function.__module__

    @property
    def key(self):
        """
        Generate a key for a node for use in dictionaries, etc. The main goal is for it to be unique, so that nodes
        with the same function name still return different keys.
        """
        if self._key is None:
            key = f'{self.function.__module__}.{self.function.__class__.__name__}.{self.function.__name__}.{self.function.__qualname__}'
            self._key = re.sub('[^\da-zA-Z]', '_', key)
        return self._key

    @key.setter
    def key(self, value):
        self._key = value

    @property
    def __doc__(self):
        """Return the docstring of the wrapped transformation rather than the docstring of the decorator object"""
        return self.function.__doc__

    def _create_graph(self, skipped_node_keys=None, pandas_on_spark_use_pandas=False):
        from flypipe.node_graph import NodeGraph
        self.node_graph = NodeGraph(self, pandas_on_spark_use_pandas=pandas_on_spark_use_pandas)
        if not skipped_node_keys:
            skipped_node_keys = []
        self.node_graph.calculate_graph_run_status(self.key, skipped_node_keys)

    def select(self, *columns):
        # TODO- if self.output_schema is defined then we should ensure each of the columns is in it.
        # otherwise if self.output_schema is not defined then we won't know the ultimate output schema
        # so can't do any validation

        cols = columns[0] if isinstance(columns[0], list) else list(columns)

        if len(cols) != len(set(cols)):
            raise ValueError(f"Duplicated columns in selection of {self.__name__}")
        return InputNode(self, cols)

    def get_node_inputs(self, outputs: Mapping[str, NodeResult]):
        inputs = {}
        for input_node in self.input_nodes:
            node_input_value = outputs[input_node.key].as_type(self.input_dataframe_type)
            inputs[input_node.get_alias()] = node_input_value.select_columns(*input_node.selected_columns).df

        return inputs

    def __call__(self, *args):
        return self.function(*args)

    def run(self, spark=None, parallel=None, inputs=None, pandas_on_spark_use_pandas=False):
        if not inputs:
            inputs = {}
        provided_inputs = {node.key: df for node, df in inputs.items()}
        self._create_graph(list(provided_inputs.keys()), pandas_on_spark_use_pandas)
        if parallel is None:
            parallel = (get_config('default_run_mode') == RunMode.PARALLEL.value)
        if parallel:
            raise NotImplementedError
        else:
            return self._run_sequential(spark, provided_inputs)

    @property
    def input_dataframe_type(self):
        return self.type

    def _run_sequential(self, spark=None, provided_inputs=None):
        if provided_inputs is None:
            provided_inputs = {}
        outputs = {key: NodeResult(spark, df, schema=None) for key, df in provided_inputs.items()}
        execution_graph = self.node_graph.copy()

        while not execution_graph.is_empty():
            runnable_nodes = execution_graph.pop_runnable_transformations()
            for runnable_node in runnable_nodes:
                if runnable_node['transformation'].key in outputs:
                    continue

                dependency_values = runnable_node['transformation'].get_node_inputs(outputs)

                result = NodeResult(
                    spark,
                    runnable_node['transformation'].process_transformation(spark, **dependency_values),
                    schema=self._get_consolidated_output_schema(
                        runnable_node['transformation'].output_schema,
                        runnable_node['output_columns']
                    )
                )

                outputs[runnable_node['transformation'].key] = result

        return outputs[self.key].as_type(self.type).df

    @classmethod
    def _get_consolidated_output_schema(cls, output_schema, output_columns):
        """
        The output schema for a transformation is currently optional. If not provided, we create a simple one from the
        set of columns selected by descendant nodes.
        """
        if output_schema:
            schema = output_schema
        elif output_columns is not None:
            columns = []
            for output_column in output_columns:
                columns.append(Column(output_column, Unknown(), ''))
            schema = Schema(columns)
        else:
            schema = None
        return schema

    def process_transformation(self, spark, **inputs):
        # TODO: apply output validation + rename function to transformation, select only necessary columns specified in self.dependencies_selected_columns
        if self.spark_context:
            parameters = {'spark': spark, **inputs}
        else:
            parameters = inputs

        return self.function(**parameters)

    def plot(self):
        self.node_graph.plot()

    def html(self, width=-1, height=1000, inputs=None, pandas_on_spark_use_pandas=False):
        from flypipe.printer.graph_html import GraphHTML
        skipped_nodes = inputs or []
        self._create_graph([node.key for node in skipped_nodes], pandas_on_spark_use_pandas)
        return GraphHTML(self.node_graph, width=width, height=height).html()


def node(type, *args, **kwargs):
    """
    Decorator factory that returns the given function wrapped inside a Node class
    """

    def decorator(func):
        kwargs['type'] = type
        return Node(func, *args, **kwargs)

    return decorator
