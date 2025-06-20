from dataclasses import dataclass
from typing import Union

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


from pyspark.sql.dataframe import DataFrame as PySparkDataFrame

from flypipe.cache.cache_context import CacheContext


@dataclass
class NodeRunContext:
    """
    NodeRunContext is a model held by each graph node that holds node information that is tied to a particular run,
    such as parameters.
    """

    parameters: dict = None
    provided_input: Union[
        PandasDataFrame, PySparkDataFrame, PandasApiDataFrame, PySparkConnectDataFrame
    ] = None
    cache_context: CacheContext = None

    def __post_init__(self):
        self.parameters = self.parameters or {}
        self.cache_context = self.cache_context or CacheContext()

    @property
    def exists_provided_input(self):
        return self.provided_input is not None
