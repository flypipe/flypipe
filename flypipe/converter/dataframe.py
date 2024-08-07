# ruff: noqa: E731
from pyspark.sql.types import StructType, StructField, StringType

from flypipe.utils import DataFrameType, dataframe_type
from logging import warning


class DataFrameConverter:
    """Converts a dataframe between pandas, pandas on spark and pyspark

    Attributes
    ----------
    spark : spark Session, default None
    """

    def __init__(self, spark=None):
        self.spark = spark

    def _convert_pandas_to_spark(self, df):
        if df.shape[0] > 0:
            return self.spark.createDataFrame(df)
        else:
            warning(
                "pyspark.errors.exceptions.base.PySparkValueError: [CANNOT_INFER_EMPTY_SCHEMA] Can not infer "
                "schema from empty Pandas Dataframe to Pyspark Dataframe => Creating empty dataset with "
                "StringType() for all columns"
            )
            schema = StructType(
                [StructField(column, StringType(), True) for column in df.columns]
            )
            return self.spark.createDataFrame([], schema=schema)

    def _strategy(self, from_type: DataFrameType, to_type: DataFrameType):
        """Defines the strategy to convert and the function to be applied in this conversion

        Parameters
        ----------
        from_type : DataFrameType
            from dataframe type
        to_type : DataFrameType
            dataframe type to be converted

        Returns
        -------
        lambda function
            the function to do the conversion
        """
        pandas_to_spark = lambda df: self._convert_pandas_to_spark(df)
        pandas_to_pandas_on_spark = lambda df: self._convert_pandas_to_spark(
            df
        ).pandas_api()

        pandas_on_spark_to_pandas = lambda df: df.to_pandas()
        pandas_on_spark_to_spark = lambda df: df.to_spark()

        spark_to_pandas = lambda df: df.toPandas()
        spark_to_pandas_on_spark = lambda df: df.pandas_api()

        return {
            DataFrameType.PANDAS: {
                DataFrameType.PYSPARK: pandas_to_spark,
                DataFrameType.PANDAS_ON_SPARK: pandas_to_pandas_on_spark,
            },
            DataFrameType.PANDAS_ON_SPARK: {
                DataFrameType.PANDAS: pandas_on_spark_to_pandas,
                DataFrameType.PYSPARK: pandas_on_spark_to_spark,
            },
            DataFrameType.PYSPARK: {
                DataFrameType.PANDAS: spark_to_pandas,
                DataFrameType.PANDAS_ON_SPARK: spark_to_pandas_on_spark,
            },
        }[from_type][to_type]

    def convert(self, df, to_type: DataFrameType):
        """Do the conversion of the given dataframe to the desired type

        Parameters
        ----------
        df : DataFrame
            dataframe to be converted
        to_type : DataFrameType
            dataframe type to be converted

        Returns
        -------
        dataframe
            converted dataframe
        """

        from_type = dataframe_type(df)

        if from_type == to_type:
            return df

        return self._strategy(from_type, to_type)(df)
