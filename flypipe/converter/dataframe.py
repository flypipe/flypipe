from flypipe.utils import DataFrameType, dataframe_type


class DataFrameConverter:  # pylint: disable=too-few-public-methods
    """Converts a dataframe between pandas, pandas on spark and pyspark

    Attributes
    ----------
    spark : spark Session, default None
    """

    def __init__(self, spark=None):
        self.spark = spark

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
        # pylint: disable=unnecessary-lambda-assignment,unnecessary-lambda
        pandas_to_spark = lambda df: self.spark.createDataFrame(df)
        pandas_to_pandas_on_spark = lambda df: self.spark.createDataFrame(
            df
        ).pandas_api()

        pandas_on_spark_to_pandas = lambda df: df.to_pandas()
        pandas_on_spark_to_spark = lambda df: df.to_spark()

        spark_to_pandas = lambda df: df.toPandas()
        spark_to_pandas_on_spark = lambda df: df.pandas_api()
        # pylint: enable=unnecessary-lambda-assignment

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
