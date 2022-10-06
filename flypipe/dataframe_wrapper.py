from flypipe.converter.dataframe import DataFrameConverter
from flypipe.converter.schema import SchemaConverter
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
        self.spark_data = None
        self.type = dataframe_type(df)
        if self.type == DataFrameType.PANDAS:
            self.pandas_data = df
        elif self.type == DataFrameType.PYSPARK:
            self.pyspark_data = df
        elif self.type == DataFrameType.PANDAS_ON_SPARK:
            self.pandas_data = df
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
            raise ValueError(f'Type {df_type} not supported')

    def as_pandas(self):
        # FIXME: return deep copy of dataframe
        if not self.pandas_data:
            self.pandas_data = self.dataframe_converter.convert(self.pyspark_data, DataFrameType.PANDAS)
            if self.schema:
                self.pandas_data = SchemaConverter.cast(self.pandas_data, DataFrameType.PANDAS, self.schema)
        # FIXME: implement tests
        return self.pandas_data.copy(deep=True)

    def as_pandas_on_spark(self):
        # FIXME: convert to spark and then back again to pandas on spark
        if not self.pandas_data:
            self.pandas_data = self.dataframe_converter.convert(self.pyspark_data, DataFrameType.PANDAS_ON_SPARK)
            if self.schema:
                self.pandas_data = SchemaConverter.cast(self.pandas_data, DataFrameType.PANDAS_ON_SPARK, self.schema)
        # FIXME: implement tests
        return self.pandas_data.toSpark().to_pandas_on_spark()

    def as_pyspark(self):
        if not self.pyspark_data:
            self.pyspark_data = self.dataframe_converter.convert(self.pandas_data, DataFrameType.PYSPARK)
            if self.schema:
                self.pyspark_data = SchemaConverter.cast(self.pyspark_data, DataFrameType.PYSPARK, self.schema)
        return self.pyspark_data
