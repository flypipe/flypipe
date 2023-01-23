from abc import ABC, abstractmethod

from flypipe.schema.types import Type
from flypipe.utils import dataframe_type, DataFrameType


class DataFrameWrapper(ABC):
    """
    Flypipe dataframe, currently it's just a very thin wrapper around a pandas/spark/etc dataframe that knows what
    exact concrete dataframe type it's storing is.
    """

    DF_TYPE = None
    FLYPIPE_TYPE_TO_DF_TYPE_MAP = {}

    def __init__(self, spark, df):
        self.spark = spark
        self.df = df  # pylint: disable=invalid-name

    @classmethod
    def get_instance(cls, spark, df):
        # We need to do imports of the various df types within the function to avoid circular imports as they in turn
        # import dataframe_wrapper
        # pylint: disable=import-outside-toplevel,cyclic-import
        df_type = dataframe_type(df)
        if df_type == DataFrameType.PANDAS:
            from flypipe.dataframe.pandas_dataframe_wrapper import (
                PandasDataFrameWrapper,
            )

            df_instance = PandasDataFrameWrapper

        elif df_type == DataFrameType.PYSPARK:
            from flypipe.dataframe.spark_dataframe_wrapper import SparkDataFrameWrapper

            df_instance = SparkDataFrameWrapper

        elif df_type == DataFrameType.PANDAS_ON_SPARK:
            import pyspark.pandas as ps

            ps.set_option("compute.ops_on_diff_frames", True)
            from flypipe.dataframe.pandas_on_spark_dataframe_wrapper import (
                PandasOnSparkDataFrameWrapper,
            )

            df_instance = PandasOnSparkDataFrameWrapper
        else:
            raise ValueError(f"No flypipe dataframe type found for dataframe {df_type}")
        # pylint: enable=import-outside-toplevel,cyclic-import
        return df_instance(spark, df)

    def get_df(self):
        return self.df

    def select_columns(self, *columns):
        """
        Accepts either a collection of columns either as *args or a list:
        dataframe_wrapper.select_columns('col1', 'col2', ...)
        dataframe_wrapper.select_columns(['col1', 'col2', ...])

        Returns a dataframe with just those specific columns selected.
        Throw a KeyError if any of the requested columns do not exist in the underlying dataframe.
        """
        if columns and isinstance(columns[0], list):
            columns = columns[0]
        return self.__class__(self.spark, self._select_columns(columns))

    @abstractmethod
    def _select_columns(self, columns):
        """Return a copy of the underlying dataframe with only the supplied columns selected"""
        raise NotImplementedError

    @abstractmethod
    def get_column_flypipe_type(self, target_column):
        raise NotImplementedError

    def cast_column(self, column: str, flypipe_type: Type):
        result = None
        if self.get_column_flypipe_type(column).name != flypipe_type.name:
            # Check if the column already has the requested type
            try:
                result = getattr(self, f"_cast_column_{flypipe_type.key()}")(
                    column, flypipe_type
                )
            except AttributeError as exc:
                if flypipe_type.key() in self.FLYPIPE_TYPE_TO_DF_TYPE_MAP:
                    df_type = self.FLYPIPE_TYPE_TO_DF_TYPE_MAP[flypipe_type.key()]
                    result = self._cast_column(column, flypipe_type, df_type)
                else:
                    raise TypeError(
                        f"Unable to cast to flypipe type {flypipe_type.name}- no dataframe type registered"
                    ) from exc
        return result

    @abstractmethod
    def _cast_column(self, column: str, flypipe_type: Type, df_type):
        raise NotImplementedError

    def _cast_column_unknown(
        self, column: str, flypipe_type: Type
    ):  # pylint: disable=unused-argument
        """If we don't know the type let's do nothing"""
        return
