from flypipe.utils import DataFrameType, dataframe_type


class DataFrameConverter:

    def __init__(self, spark=None):
        self.spark = spark

    def _strategy(self, from_type: DataFrameType, to_type: DataFrameType):
        pandas_to_spark = lambda df: self.spark.createDataFrame(df)
        pandas_to_pandas_on_spark = lambda df: self.spark.createDataFrame(df).to_pandas_on_spark()

        pandas_on_spark_to_pandas = lambda df: df.to_pandas()
        pandas_on_spark_to_spark = lambda df: df.to_spark()

        spark_to_pandas = lambda df: df.toPandas()
        spark_to_pandas_on_spark = lambda df: df.to_pandas_on_spark()

        return {
            DataFrameType.PANDAS.value: {
                DataFrameType.PYSPARK.value: pandas_to_spark,
                DataFrameType.PANDAS_ON_SPARK.value: pandas_to_pandas_on_spark
            },

            DataFrameType.PANDAS_ON_SPARK.value: {
                DataFrameType.PANDAS.value: pandas_on_spark_to_pandas,
                DataFrameType.PYSPARK.value: pandas_on_spark_to_spark
            },

            DataFrameType.PYSPARK.value: {
                DataFrameType.PANDAS.value: spark_to_pandas,
                DataFrameType.PANDAS_ON_SPARK.value: spark_to_pandas_on_spark
            }
        }[from_type.value][to_type.value]

    def convert(self, df, to_type: DataFrameType):
        from_type = dataframe_type(df)

        if from_type == to_type:
            return df

        return self._strategy(from_type, to_type)(df)
