from flypipe.schema import Schema
from flypipe.utils import dataframe_type, DataFrameType
from abc import ABC, abstractmethod


class DataFrameWrapper(ABC):
    """
    Flypipe dataframe, currently it's just a very thin wrapper around a pandas/spark/etc dataframe that knows what
    exact concrete dataframe type it's storing is.
    """
    TYPE = None

    def __init__(self, spark, df, schema):
        self.spark = spark
        self.df = df
        self.schema = schema
        if self.schema:
            self.df = self._select_columns([column.name for column in schema.columns])

    @classmethod
    def get_instance(cls, spark, df, schema):
        # Avoid circular imports by doing local imports here
        from flypipe.dataframe.pandas_dataframe_wrapper import PandasDataFrameWrapper
        from flypipe.dataframe.pandas_on_spark_dataframe_wrapper import PandasOnSparkDataFrameWrapper
        from flypipe.dataframe.spark_dataframe_wrapper import SparkDataFrameWrapper
        df_type = dataframe_type(df)
        if df_type == DataFrameType.PANDAS:
            df_instance = PandasDataFrameWrapper
        elif df_type == DataFrameType.PYSPARK:
            df_instance = SparkDataFrameWrapper
        elif df_type == DataFrameType.PANDAS_ON_SPARK:
            df_instance = PandasOnSparkDataFrameWrapper
        else:
            raise ValueError(f'No flypipe dataframe type found for dataframe {df_type}')
        return df_instance(spark, df, schema)

    def select_columns(self, *columns):
        """
        Accepts either a collection of columns either as *args or a list:
        dataframe_wrapper.select_columns('col1', 'col2', ...)
        dataframe_wrapper.select_columns(['col1', 'col2', ...])

        Returns a new instance of the class wrapped around the dataframe with just those specific columns selected.
        """
        if columns and isinstance(columns[0], list):
            columns = columns[0]
        new_dataframe_schema_columns = []
        for column in self.schema.columns:
            if column.name in columns:
                new_dataframe_schema_columns.append(column)
        return self.__class__(self.spark, self._select_columns(columns), Schema(new_dataframe_schema_columns))

    @abstractmethod
    def _select_columns(self, columns):
        """Return a copy of the underlying dataframe with only the supplied columns selected"""
        raise NotImplementedError

    # def as_pandas(self):
    #     # FIXME: return deep copy of dataframe
    #     if self.pandas_data is None:
    #         df = self.pyspark_data if self.pandas_on_spark_data is None else self.pandas_on_spark_data
    #         self.pandas_data = self.dataframe_converter.convert(df, DataFrameType.PANDAS)
    #         if self.schema:
    #             self.pandas_data = SchemaConverter.cast(self.pandas_data, DataFrameType.PANDAS, self.schema)
    #     # FIXME: implement tests
    #     return self.pandas_data.copy(deep=True)
    #
    # def as_pandas_on_spark(self):
    #     # FIXME: convert to spark and then back again to pandas on spark
    #     if self.pandas_on_spark_data is None:
    #
    #         if self.pandas_data is None:
    #             df = self.pyspark_data
    #             self.pandas_on_spark_data = self.dataframe_converter.convert(df, DataFrameType.PANDAS_ON_SPARK)
    #         else:
    #             df = self.pandas_data
    #             self.pandas_on_spark_data = df
    #
    #         if self.schema:
    #             self.pandas_on_spark_data = SchemaConverter.cast(self.pandas_on_spark_data,
    #                                                              dataframe_type(self.pandas_on_spark_data),
    #                                                              self.schema)
    #     # FIXME: implement tests
    #     return self.pandas_on_spark_data.copy(deep=True)
    #
    # def as_pyspark(self):
    #     if self.pyspark_data is None:
    #         df = self.pandas_on_spark_data if self.pandas_data is None else self.pandas_data
    #         self.pyspark_data = self.dataframe_converter.convert(df, DataFrameType.PYSPARK)
    #         if self.schema:
    #             self.pyspark_data = SchemaConverter.cast(self.pyspark_data, DataFrameType.PYSPARK, self.schema)
    #     return self.pyspark_data
