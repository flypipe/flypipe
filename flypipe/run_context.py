from copy import copy
from dataclasses import dataclass, field
from typing import Mapping, Union

from pandas import DataFrame as PandasDataFrame
from pyspark.pandas.frame import DataFrame as PandasApiDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as PySparkDataFrame

from flypipe.dependency.preprocess_mode import PreProcessMode
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
    pyspark_use_sparkleframe: bool = False
    parameters: dict = None
    cache_modes: dict = None
    dependencies_preprocess_modes: Union[dict, PreProcessMode] = None
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
        self.pyspark_use_sparkleframe = (
            False
            if self.pyspark_use_sparkleframe is None
            else self.pyspark_use_sparkleframe
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
        df: Union[PandasDataFrame, PySparkDataFrame, PandasApiDataFrame],
        schema: Schema = None,
    ):
        self.node_results[node_key] = NodeResult(self.spark, df, schema=schema)

    @property
    def skipped_node_keys(self):
        return [node.key for node in self.provided_inputs.keys()]

    def get_run_preprocess_mode(self) -> PreProcessMode:
        """
        Returns the PreProcessMode for the whole run
        """

        # it is a PreProcessMode to apply to all dependencies
        if isinstance(self.dependencies_preprocess_modes, PreProcessMode):
            return self.dependencies_preprocess_modes

        # By default all PreProcesses ar active
        return PreProcessMode.ACTIVE

    def get_dependency_preprocess_mode(self, parent_node, dependency_node):
        """
        Returns the PreProcessMode for a specific dependency (dependency_node) of a node (parent_node).
        """
        if isinstance(self.dependencies_preprocess_modes, dict):
            if parent_node in self.dependencies_preprocess_modes:
                if (
                    dependency_node.node
                    in self.dependencies_preprocess_modes[parent_node]
                ):
                    return self.dependencies_preprocess_modes[parent_node][
                        dependency_node.node
                    ]

        # By default, it is active
        return PreProcessMode.ACTIVE
