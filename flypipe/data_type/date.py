from typing import Union

import numpy as np
import pandas as pd
import pyspark.pandas as ps
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType

from flypipe.data_type.type import Type
from flypipe.utils import dataframe_type, DataFrameType, get_schema


class Date(Type):
    """Casts dataframe to approppriate date format in pandas and spark dataframes.

    Attributes
    ----------
    fmt : str
        the format of the date in the dataframe to be converted, useful for pandas dataframe
        with dates that datatype is of object, this fmt will help casting dates in string
    """

    spark_type = DateType()
    pandas_type = np.dtype("<M8[ns]")

    def __init__(self, fmt: str = "%Y-%m-%d"):
        self.fmt = fmt

    def _cast_pyspark(self, df, column: str):
        df = df.withColumn(column, F.to_date(F.col(column), self.fmt))
        return df

    def _cast_pandas(self, df, column: str):
        df[column] = pd.to_datetime(df[column], format=self.fmt).astype(self.pandas_type)
        return df

    def _cast_pandas_on_spark(self, df, column: str):
        df[column] = ps.to_datetime(df[column], format=self.fmt).astype(self.pandas_type)
        return df
