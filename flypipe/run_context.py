from copy import copy
from dataclasses import dataclass, field
from typing import Mapping, Union

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
from flypipe.config import get_config, RunMode
from flypipe.node_result import NodeResult
from flypipe.schema import Schema


@dataclass
class RunContext:
    """
    RunContext is a model held by each run that holds run information that is tied to a particular run,
    such as parameters, cache mode and inputs.
    """

    spark: SparkSession = None
    parallel: bool = None
    provided_inputs: dict = None
    pandas_on_spark_use_pandas: bool = False
    parameters: dict = None
    cache_modes: dict = None
    dependencies_preprocess_modes: Union[dict, PreprocessMode] = None
    node_results: Mapping[str, NodeResult] = field(init=False, default=None)

    def __post_init__(self):
        self.parallel = (
            get_config("default_run_mode") == RunMode.PARALLEL.value
            if self.parallel is None
            else self.parallel
        )

        self.provided_inputs = self.provided_inputs or {}
        self.pandas_on_spark_use_pandas = (
            False
            if self.pandas_on_spark_use_pandas is None
            else self.pandas_on_spark_use_pandas
        )
        self.parameters = self.parameters or {}
        self.cache_modes = self.cache_modes or {}

        self.dependencies_preprocess_modes = self.dependencies_preprocess_modes or {}
        self.node_results = {
            node.key: NodeResult(self.spark, df, schema=None)
            for node, df in self.provided_inputs.items()
        }

    def copy(self):
        return copy(self)

    def update_node_results(
        self,
        node_key: str,
        df: Union[
            PandasDataFrame,
            PySparkDataFrame,
            PandasApiDataFrame,
            PySparkConnectDataFrame,
        ],
        schema: Schema = None,
    ):
        self.node_results[node_key] = NodeResult(self.spark, df, schema=schema)

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
