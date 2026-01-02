# ruff: noqa: E731
from typing import Union
from logging import warning

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
from snowflake.snowpark.session import Session as SnowflakeSession

from flypipe.utils import DataFrameType, dataframe_type


class DataFrameConverter:
    """Converts a dataframe between pandas, pandas on spark, pyspark and snowflake

    Attributes
    ----------
    session : Union[snowflake.snowpark.session.Session, pyspark.sql.SparkSession], default None
        The session to use for dataframe conversions (Snowflake or Spark)
    """

    def __init__(self, session: Union[SnowflakeSession, SparkSession] = None):
        self.session = session

    def _convert_pandas_to_spark(self, df):
        if df.shape[0] > 0:
            return self.session.createDataFrame(df)
        else:
            warning(
                "pyspark.errors.exceptions.base.PySparkValueError: [CANNOT_INFER_EMPTY_SCHEMA] Can not infer "
                "schema from empty Pandas Dataframe to Pyspark Dataframe => Creating empty dataset with "
                "StringType() for all columns"
            )
            schema = StructType(
                [StructField(column, StringType(), True) for column in df.columns]
            )
            return self.session.createDataFrame([], schema=schema)

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
