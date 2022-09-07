from typing import Union
import numpy as np

from flypipe.data_type.type import Type
from flypipe.utils import dataframe_type, DataFrameType, get_schema
import pyspark.sql.functions as F
import pyspark.pandas as ps
import pandas as pd
from pyspark.sql.types import DateType, StringType


class Date(Type):
    """Casts dataframe to approppriate date format in pandas and spark dataframes.

    Attributes
    ----------
    fmt : str
        the format of the date in the dataframe to be converted, useful for pandas dataframe
        with dates that datatype is of object, this fmt will help casting dates in string
    """

    spark_data_type = DateType

    def __init__(self, fmt: str = "%Y-%m-%d"):
        self.fmt = fmt

    def cast(self, df, column: Union[str, list]):
        """Cast the dataframes columns to date for spark dataframes or
        to datetime64[ns] for pandas dataframes

        Parameters
        ----------
        df : dataframe
            dataframe to have column(s) casted
        column: str or list
            column(s) to be casted

        Returns
        -------
        dataframe
            dataframe with given column(s) casted to the DataType defined by the child in spark_data_type

        """
        columns_to_cast = self.columns(df, column)

        schema = get_schema(df, columns_to_cast)

        if dataframe_type(df) == DataFrameType.PYSPARK:

            columns_to_cast = {
                column: data_type
                for column, data_type in schema.items()
                if data_type != self.spark_data_type and column in columns_to_cast
            }

            for column, data_type in columns_to_cast.items():
                if data_type == StringType():
                    df = df.withColumn(column, F.to_date(F.col(column), self.fmt))
                else:
                    df = df.withColumn(column, F.col(column).cast(DateType()))

        else:
            columns_to_cast = {
                column: data_type
                for column, data_type in schema.items()
                if data_type != np.dtype("<M8[ns]") and column in columns_to_cast
            }

            for column, data_type in columns_to_cast.items():
                if data_type == np.dtype("object_"):
                    if dataframe_type(df) == DataFrameType.PANDAS_ON_SPARK:
                        df[column] = ps.to_datetime(df[column], format=self.fmt)
                    else:
                        df[column] = pd.to_datetime(df[column], format=self.fmt)

            columns_to_cast = list(columns_to_cast.keys())
            df[columns_to_cast] = df[columns_to_cast].astype(np.dtype("<M8[ns]"))

        return df
