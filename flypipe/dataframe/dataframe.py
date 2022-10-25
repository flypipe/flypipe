from flypipe.converter.dataframe import DataFrameConverter
from flypipe.converter.schema import SchemaConverter
from flypipe.exceptions import DataframeTypeNotSupportedError
from flypipe.utils import dataframe_type, DataFrameType
from abc import ABC, abstractmethod


class DataFrame(ABC):
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
            self.df = self.select_columns(schema.columns)
        self.dataframe_converter = DataFrameConverter(self.spark)

    @classmethod
    def get_instance(cls, spark, df, schema):
        # Avoid circular imports by doing local imports here
        from flypipe.dataframe.pandas import PandasDataFrame
        from flypipe.dataframe.pandas_on_spark import PandasOnSparkDataFrame
        from flypipe.dataframe.spark import SparkDataFrame
        df_type = dataframe_type(df)
        if df_type == DataFrameType.PANDAS:
            df_instance = PandasDataFrame
        elif df_type == DataFrameType.PYSPARK:
            df_instance = SparkDataFrame
        elif df_type == DataFrameType.PANDAS_ON_SPARK:
            df_instance = PandasOnSparkDataFrame
        else:
            raise ValueError(f'No flypipe dataframe type found for dataframe {df_type}')
        return df_instance(spark, df, schema)

    @abstractmethod
    def select_columns(self, *columns):
        raise NotImplementedError

    def as_type(self, df_type: DataFrameType):
        if self.TYPE == df_type:
            dataframe = self
        else:
            dataframe = DataFrame.get_instance(self.spark, self.dataframe_converter.convert(self.df, df_type), self.schema)
            if self.schema:
                dataframe = SchemaConverter.cast(dataframe, self.TYPE, self.schema)
        return dataframe

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
