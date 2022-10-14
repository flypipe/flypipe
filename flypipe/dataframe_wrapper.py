from flypipe.converter.dataframe import DataFrameConverter
from flypipe.converter.schema import SchemaConverter
from flypipe.exceptions import DataframeTypeNotSupportedError
from flypipe.utils import dataframe_type, DataFrameType


class DataframeWrapper:
    """
    Flypipe dataframe, currently it's just a very thin wrapper around a pandas/spark/etc dataframe that knows what
    exact concrete dataframe type it's storing is.
    """

    def __init__(self, spark, df, schema):
        self.spark = spark
        self.dataframe_converter = DataFrameConverter(self.spark)
        self.schema = schema
        self.pandas_data = None
        self.pandas_on_spark_data = None
        self.pyspark_data = None
        self.type = dataframe_type(df)
        if self.type == DataFrameType.PANDAS:
            self.pandas_data = df
            if self.schema:
                self.pandas_data = self.pandas_data[[col.name for col in self.schema.columns]]

        elif self.type == DataFrameType.PYSPARK:
            self.pyspark_data = df
            if self.schema:
                self.pyspark_data = self.pyspark_data.select([col.name for col in self.schema.columns])

        elif self.type == DataFrameType.PANDAS_ON_SPARK:
            self.pandas_on_spark_data = df
            if self.schema:
                self.pandas_on_spark_data = self.pandas_on_spark_data[[col.name for col in self.schema.columns]]
        else:
            raise ValueError(f'Type {self.type} not supported')

    def as_type(self, df_type: DataFrameType):
        if df_type == DataFrameType.PANDAS:
            return self.as_pandas()
        elif df_type == DataFrameType.PYSPARK:
            return self.as_pyspark()
        elif df_type == DataFrameType.PANDAS_ON_SPARK:
            return self.as_pandas_on_spark()
        else:
            raise DataframeTypeNotSupportedError(f'Type {df_type} not supported')

    def as_pandas(self):
        # FIXME: return deep copy of dataframe
        if self.pandas_data is None:
            df = self.pyspark_data if self.pandas_on_spark_data is None else self.pandas_on_spark_data
            self.pandas_data = self.dataframe_converter.convert(df, DataFrameType.PANDAS)
            if self.schema:
                self.pandas_data = SchemaConverter.cast(self.pandas_data, DataFrameType.PANDAS, self.schema)
        # FIXME: implement tests
        return self.pandas_data.copy(deep=True)

    def as_pandas_on_spark(self):
        # FIXME: convert to spark and then back again to pandas on spark
        if self.pandas_on_spark_data is None:

            if self.pandas_data is None:
                df = self.pyspark_data
                self.pandas_on_spark_data = self.dataframe_converter.convert(df, DataFrameType.PANDAS_ON_SPARK)
            else:
                df = self.pandas_data
                self.pandas_on_spark_data = df

            if self.schema:
                self.pandas_on_spark_data = SchemaConverter.cast(self.pandas_on_spark_data,
                                                                 dataframe_type(self.pandas_on_spark_data),
                                                                 self.schema)
        # FIXME: implement tests
        return self.pandas_on_spark_data.copy(deep=True)

    def as_pyspark(self):
        if self.pyspark_data is None:
            df = self.pandas_on_spark_data if self.pandas_data is None else self.pandas_data
            self.pyspark_data = self.dataframe_converter.convert(df, DataFrameType.PYSPARK)
            if self.schema:
                self.pyspark_data = SchemaConverter.cast(self.pyspark_data, DataFrameType.PYSPARK, self.schema)
        return self.pyspark_data
