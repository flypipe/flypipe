from flypipe.spark.context import get_spark_session


class SparkDataframe:

    @classmethod
    def to_pandas(cls, spark_df):
        return spark_df.toPandas()

    @classmethod
    def from_pandas(cls, pandas_df):
        spark = get_spark_session()
        return spark.createDataFrame(pandas_df)
