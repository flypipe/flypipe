from flypipe.converter.dataframe import DataFrameConverter
from flypipe.converter.schema import SchemaConverter
from flypipe.exceptions import DataframeTypeNotSupportedError, SelectionNotFoundInTable
from flypipe.utils import dataframe_type, DataFrameType


class DataframeWrapper:
    """
    Flypipe dataframe, currently it's just a very thin wrapper around a pandas/spark/etc dataframe that knows what
    exact concrete dataframe type it's storing is.
    """

    def __init__(self, spark, node_name, df, schema):
        self.spark = spark
        self.node_name = node_name
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

    def select(self, df, selected_columns: [str]=None):

        if not selected_columns:
            return df

        df_cols = [col for col in df.columns]

        if not set(selected_columns).issubset(set(df_cols)):
            raise SelectionNotFoundInTable(self.node_name, df_cols, selected_columns)

        return df[selected_columns]


    def as_type(self, df_type: DataFrameType, selected_columns: [str]=None):
        if df_type == DataFrameType.PANDAS:
            return self.select(self.as_pandas(), selected_columns)

        elif df_type == DataFrameType.PYSPARK:
            return self.select(self.as_pyspark(), selected_columns)

        elif df_type == DataFrameType.PANDAS_ON_SPARK:
            return self.select(self.as_pandas_on_spark(), selected_columns)
        else:
            raise DataframeTypeNotSupportedError(f'Type {df_type} not supported')

    def as_pandas(self):
        if self.pandas_data is None:
            df = self.pyspark_data if self.pandas_on_spark_data is None else self.pandas_on_spark_data
            self.pandas_data = self.dataframe_converter.convert(df, DataFrameType.PANDAS)
            if self.schema:
                self.pandas_data = SchemaConverter.cast(self.pandas_data, DataFrameType.PANDAS, self.schema)
        # FIXME: implement tests
        return self.pandas_data.copy(deep=True)

    def as_pandas_on_spark(self):
        if self.pandas_on_spark_data is None:

            if self.pandas_data is None:
                self.pandas_on_spark_data = self.dataframe_converter.convert(self.pyspark_data,
                                                                             DataFrameType.PANDAS_ON_SPARK)
            else:
                self.pandas_on_spark_data = self.as_pandas()

            if self.schema:
                self.pandas_on_spark_data = SchemaConverter.cast(self.pandas_on_spark_data,
                                                                 dataframe_type(self.pandas_on_spark_data),
                                                                 self.schema)

            return self.pandas_on_spark_data
        else:
            # FIXME: implement tests
            return self.pandas_on_spark_data.copy(deep=True)

    def as_pyspark(self):
        if self.pyspark_data is None:
            df = self.pandas_on_spark_data if self.pandas_data is None else self.pandas_data
            self.pyspark_data = self.dataframe_converter.convert(df, DataFrameType.PYSPARK)
            if self.schema:
                self.pyspark_data = SchemaConverter.cast(self.pyspark_data, DataFrameType.PYSPARK, self.schema)
        return self.pyspark_data
