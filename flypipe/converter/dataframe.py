from __future__ import annotations

# ruff: noqa: E731
from typing import Union, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from snowflake.snowpark.session import Session as SnowflakeSession

from flypipe.utils import DataFrameType, dataframe_type, get_logger
from flypipe.exceptions import UnsupportedConversionError

logger = get_logger()


class DataFrameConverter:
    """Converts a dataframe between pandas, pandas on spark, pyspark and snowflake

    Attributes
    ----------
    session : Union[snowflake.snowpark.session.Session, pyspark.sql.SparkSession], default None
        The session to use for dataframe conversions (Snowflake or Spark)
    
    Notes
    -----
    Direct conversions are supported for:
    - Pandas <-> PySpark
    - Pandas <-> Pandas-on-Spark
    - Pandas <-> Snowpark
    - Pandas-on-Spark <-> PySpark
    
    Conversions between PySpark and Snowpark require going through Pandas as an intermediate step.
    """

    def __init__(self, session: Union["SnowflakeSession", "SparkSession"] = None):
        self.session = session

    def _convert_pandas_to_spark(self, df):
        """Convert Pandas DataFrame to PySpark DataFrame"""
        from pyspark.sql import SparkSession
        from pyspark.sql.connect.session import SparkSession as SparkConnectSession
        from pyspark.sql.types import StructType, StructField, StringType

        if not isinstance(self.session, SparkSession) and not isinstance(self.session, SparkConnectSession):
            raise ValueError(
                "PySpark SparkSession required to convert to PySpark DataFrame"
            )
        if df.shape[0] > 0:
            return self.session.createDataFrame(df)
        else:
            logger.warning(
                "pyspark.errors.exceptions.base.PySparkValueError: [CANNOT_INFER_EMPTY_SCHEMA] Can not infer "
                "schema from empty Pandas Dataframe to Pyspark Dataframe => Creating empty dataset with "
                "StringType() for all columns"
            )
            schema = StructType(
                [StructField(column, StringType(), True) for column in df.columns]
            )
            return self.session.createDataFrame([], schema=schema)

    def _convert_pandas_to_snowpark(self, df):
        """Convert Pandas DataFrame to Snowpark DataFrame"""
        from snowflake.snowpark.session import Session as SnowflakeSession
        
        if not isinstance(self.session, SnowflakeSession):
            raise ValueError(
                "Snowflake session required to convert to Snowpark DataFrame"
            )
        return self.session.create_dataframe(df)

    def _convert_snowpark_to_pandas(self, df):
        """Convert Snowpark DataFrame to Pandas DataFrame"""
        return df.to_pandas()

    def _convert_snowpark_to_spark(self, df):
        """
        Convert Snowpark DataFrame to PySpark DataFrame.
        
        Raises
        ------
        UnsupportedConversionError
            Direct conversion is not supported, convert to Pandas first
        """
        raise UnsupportedConversionError(
            "Direct conversion from SNOWPARK to PYSPARK is not supported."
        )

    def _convert_snowpark_to_pandas_on_spark(self, df):
        """
        Convert Snowpark DataFrame to Pandas-on-Spark DataFrame.
        
        Raises
        ------
        UnsupportedConversionError
            Direct conversion is not supported, convert to Pandas first
        """
        raise UnsupportedConversionError(
            "Direct conversion from SNOWPARK to PANDAS_ON_SPARK is not supported."
        )

    def _convert_spark_to_snowpark(self, df):
        """
        Convert PySpark DataFrame to Snowpark DataFrame.
        
        Raises
        ------
        UnsupportedConversionError
            Direct conversion is not supported, convert to Pandas first
        """
        raise UnsupportedConversionError(
            "Direct conversion from PYSPARK to SNOWPARK is not supported. "
            "Please convert to Pandas as an intermediate step."
        )

    def _convert_pandas_on_spark_to_snowpark(self, df):
        """
        Convert Pandas-on-Spark DataFrame to Snowpark DataFrame.
        
        Raises
        ------
        UnsupportedConversionError
            Direct conversion is not supported, convert to Pandas first
        """
        raise UnsupportedConversionError(
            "Direct conversion from PANDAS_ON_SPARK to SNOWPARK is not supported. "
            "Please convert to Pandas as an intermediate step."
        )

    def _convert_pandas_on_spark_to_pandas(self, df):
        """Convert Pandas-on-Spark DataFrame to Pandas DataFrame"""
        return df.to_pandas()

    def _convert_pandas_on_spark_to_spark(self, df):
        """Convert Pandas-on-Spark DataFrame to PySpark DataFrame"""
        return df.to_spark()

    def _convert_spark_to_pandas(self, df):
        """Convert PySpark DataFrame to Pandas DataFrame"""
        return df.toPandas()

    def _convert_spark_to_pandas_on_spark(self, df):
        """Convert PySpark DataFrame to Pandas-on-Spark DataFrame"""
        return df.pandas_api()

    def _convert_pandas_to_pandas_on_spark(self, df):
        """Convert Pandas DataFrame to Pandas-on-Spark DataFrame"""
        return self._convert_pandas_to_spark(df).pandas_api()

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
            
        Raises
        ------
        UnsupportedConversionError
            If direct conversion between the types is not supported
        """
        # Pandas conversions
        pandas_to_spark = lambda df: self._convert_pandas_to_spark(df)
        pandas_to_pandas_on_spark = lambda df: self._convert_pandas_to_pandas_on_spark(df)
        pandas_to_snowpark = lambda df: self._convert_pandas_to_snowpark(df)

        # Pandas-on-Spark conversions
        pandas_on_spark_to_pandas = lambda df: self._convert_pandas_on_spark_to_pandas(df)
        pandas_on_spark_to_spark = lambda df: self._convert_pandas_on_spark_to_spark(df)
        pandas_on_spark_to_snowpark = lambda df: self._convert_pandas_on_spark_to_snowpark(df)

        # PySpark conversions
        spark_to_pandas = lambda df: self._convert_spark_to_pandas(df)
        spark_to_pandas_on_spark = lambda df: self._convert_spark_to_pandas_on_spark(df)
        spark_to_snowpark = lambda df: self._convert_spark_to_snowpark(df)

        # Snowpark conversions
        snowpark_to_pandas = lambda df: self._convert_snowpark_to_pandas(df)
        snowpark_to_spark = lambda df: self._convert_snowpark_to_spark(df)
        snowpark_to_pandas_on_spark = lambda df: self._convert_snowpark_to_pandas_on_spark(df)

        conversion_map = {
            DataFrameType.PANDAS: {
                DataFrameType.PYSPARK: pandas_to_spark,
                DataFrameType.PANDAS_ON_SPARK: pandas_to_pandas_on_spark,
                DataFrameType.SNOWPARK: pandas_to_snowpark,
            },
            DataFrameType.PANDAS_ON_SPARK: {
                DataFrameType.PANDAS: pandas_on_spark_to_pandas,
                DataFrameType.PYSPARK: pandas_on_spark_to_spark,
                DataFrameType.SNOWPARK: pandas_on_spark_to_snowpark,
            },
            DataFrameType.PYSPARK: {
                DataFrameType.PANDAS: spark_to_pandas,
                DataFrameType.PANDAS_ON_SPARK: spark_to_pandas_on_spark,
                DataFrameType.SNOWPARK: spark_to_snowpark,
            },
            DataFrameType.SNOWPARK: {
                DataFrameType.PANDAS: snowpark_to_pandas,
                DataFrameType.PYSPARK: snowpark_to_spark,
                DataFrameType.PANDAS_ON_SPARK: snowpark_to_pandas_on_spark,
            },
        }

        if from_type not in conversion_map:
            raise ValueError(f"Unknown source dataframe type: {from_type}")
        
        if to_type not in conversion_map[from_type]:
            raise ValueError(
                f"No conversion path defined from {from_type} to {to_type}"
            )

        return conversion_map[from_type][to_type]

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
