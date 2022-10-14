import numpy as np
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType

from flypipe.data_type import Date


class Timestamp(Date):
    """Casts dataframe to approppriate timestamp format in pandas and spark dataframes.

    Attributes
    ----------
    fmt : str
        the format of the date in the dataframe to be converted, useful for pandas dataframe
        with timestamp that datatype is of object, this fmt will help casting timestamp using appropriate format
    """

    spark_type = TimestampType()
    pandas_type = np.dtype("<M8[ns]")

    def __init__(self, fmt: str = "%Y-%m-%d %H:%M:%S"): # yyyy-MM-dd HH:mm:ss
        self.fmt = fmt
        super().__init__(fmt=self.fmt)


    def __repr__(self):
        return f"{self.__class__.__name__}(fmt={self.fmt})"

    def _cast_pyspark(self, df, column: str):
        df = df.withColumn(column, F.to_timestamp(F.col(column), self.fmt))
        return df
