from dataclasses import dataclass, field
from typing import Union, TYPE_CHECKING

if TYPE_CHECKING:
    from flypipe.node import Node

from pandas import DataFrame as PandasDataFrame

from flypipe.utils import sparkleframe_is_active

if sparkleframe_is_active():
    # if using sparkleframe activate, it will fail because they do not implement pyspark.pandas
    from pandas import DataFrame as PandasApiDataFrame

    # if using sparkleframe activate, it will fail because they do not implement pyspark.sql.connect
    from pyspark.sql.dataframe import DataFrame as PySparkConnectDataFrame
else:
    from pyspark.pandas.frame import DataFrame as PandasApiDataFrame
    from pyspark.sql.connect.dataframe import DataFrame as PySparkConnectDataFrame


from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as PySparkDataFrame

from flypipe.dependency.preprocess_mode import PreprocessMode
from flypipe.config import get_config
from flypipe.node_result import NodeResult

class Autodict(dict):
    def __missing__(self, key):
        value = self[key] = type(self)()
        return value

@dataclass
class RunContext:
    """
    RunContext is a model held by each run that holds run information that is tied to a particular run,
    such as parameters, cache mode and inputs.
    """

    spark: SparkSession = None
    max_workers: int = 1
    provided_inputs: dict = None
    pandas_on_spark_use_pandas: bool = False
    parameters: dict = None
    cache_modes: dict = None
    dependencies_preprocess_modes: Union[dict, PreprocessMode] = None
    debug: bool = False
    node_results: Autodict = field(init=False, default=None)

    def copy(self):
        return RunContext(
            spark=self.spark,
            max_workers=self.max_workers,
            provided_inputs=self.provided_inputs,
            pandas_on_spark_use_pandas=self.pandas_on_spark_use_pandas,
            parameters=self.parameters,
            cache_modes=self.cache_modes,
            dependencies_preprocess_modes=self.dependencies_preprocess_modes,
            debug=self.debug,
        )

    def __post_init__(self):

        self.max_workers = self.max_workers or int(get_config("node_run_max_workers"))

        self.provided_inputs = self.provided_inputs or {}
        self.pandas_on_spark_use_pandas = (
            False
            if self.pandas_on_spark_use_pandas is None
            else self.pandas_on_spark_use_pandas
        )
        self.parameters = self.parameters or {}
        self.cache_modes = self.cache_modes or {}

        self.dependencies_preprocess_modes = self.dependencies_preprocess_modes or {}

        self.node_results = Autodict()
        for node, df in self.provided_inputs.items():
            self.node_results[node][node] = NodeResult(self.spark, df, schema=None)

    

    def update_node_results(
        self,
        node: "Node",
        df: Union[
            PandasDataFrame,
            PySparkDataFrame,
            PandasApiDataFrame,
            PySparkConnectDataFrame,
        ],
    ):
        self.node_results[node] = NodeResult(self.spark, df, schema=node.output_schema)

    def populate_cached_successors(self, provided_inputs: dict, execution_graph):
        """
        Populate node_results with cached successors of provided input nodes.
        
        For each provided input node, finds all successor nodes that are cached
        and loads their cache into node_results with nested structure:
        node_results[provided_input_node][cached_successor_node] = NodeResult
        
        Parameters
        ----------
        provided_inputs : dict
            Dictionary with Node objects as keys and DataFrames as values
        execution_graph : NodeGraph
            The execution graph containing node metadata and relationships
        """
        for input_node in provided_inputs.keys():
            # Get all successors of this input node
            if input_node.key not in execution_graph.graph:
                continue
                
            for successor_key in execution_graph.graph.successors(input_node.key):
                # Get the successor node data
                successor_data = execution_graph.get_node(successor_key)
                successor_node = successor_data["transformation"]
                cache_context = successor_data["node_run_context"].cache_context
                
                # Check if this successor is cached
                if cache_context and cache_context.exists_cache_to_load:
                    # Read from cache and store in nested structure
                    cached_df = cache_context.read(from_node=input_node, to_node=successor_node)
                    self.node_results[input_node][successor_node] = NodeResult(
                        self.spark, cached_df, schema=successor_node.output_schema
                    )

    @property
    def skipped_node_keys(self):
        return [node.key for node in self.provided_inputs.keys()]

    def get_dependency_preprocess_mode(
        self, parent_node: "Node", dependency_node: "Node"  # noqa: F821
    ):
        """
        Returns the PreprocessMode for a specific dependency (dependency_node) of a node (parent_node).
        """
        # Global PreprocessMode set for all dependency nodes
        if isinstance(self.dependencies_preprocess_modes, PreprocessMode):
            return self.dependencies_preprocess_modes

        # Specific PreprocessMode set for a specific dependency node
        if isinstance(self.dependencies_preprocess_modes, dict):
            if parent_node in self.dependencies_preprocess_modes:
                if dependency_node in self.dependencies_preprocess_modes[parent_node]:
                    return self.dependencies_preprocess_modes[parent_node][
                        dependency_node
                    ]

        # By default, it is active
        return PreprocessMode.ACTIVE
