from pyspark.sql import SparkSession

from flypipe.cache import CacheMode
from flypipe.cache.cache_context import CacheContext
from flypipe.config import get_config, RunMode


class RunContext:  # pylint: disable=too-few-public-methods
    """
    RunContext is a model held by each run that holds run information that is tied to a particular run,
    such as parameters, cache mode and inputs.
    """

    def __init__(self,
                 spark: SparkSession = None,
                 parallel: bool = None,
                 inputs: dict = None,
                 pandas_on_spark_use_pandas: bool = False,
                 parameters: dict = None,
                 cache_modes: dict = None):

        self._spark = spark
        self._parallel = get_config("default_run_mode") == RunMode.PARALLEL.value if parallel is None else parallel
        self._provided_inputs = inputs or {}
        self._pandas_on_spark_use_pandas = False if pandas_on_spark_use_pandas is None else pandas_on_spark_use_pandas
        self._parameters = parameters or {}
        self._cache_modes = cache_modes or {}

    def copy(self):
        return RunContext(
            spark=self._spark,
            parallel=self._parallel,
            inputs=self._provided_inputs,
            pandas_on_spark_use_pandas=self._pandas_on_spark_use_pandas,
            parameters=self._parameters,
            cache_modes=self._cache_modes
        )

    @property
    def spark(self):
        return self._spark

    @property
    def parallel(self):
        return self._parallel

    @property
    def provided_inputs(self):
        return {node.key: df for node, df in self._provided_inputs.items()}

    @provided_inputs.setter
    def provided_inputs(self, provided_inputs: dict):
        self._provided_inputs = {**self._provided_inputs, **provided_inputs}

    @property
    def skipped_node_keys(self):
        return list(self.provided_inputs.keys())

    @property
    def pandas_on_spark_use_pandas(self):
        return self._pandas_on_spark_use_pandas

    @property
    def cache_modes(self):
        return self._cache_modes

    @property
    def parameters(self) -> dict:
        return {node.key: params for node, params in self._parameters.items()}

    @parameters.setter
    def parameters(self, parameters: dict):
        self._parameters = parameters