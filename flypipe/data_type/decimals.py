import numpy as np
import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType

from flypipe.data_type.type import Type


class Decimals(Type):
    """Casts dataframe to approppriate Decimal
    If dataframe is pyspark, casts to DecimalType(precision, scale)
    If dataframe is pandas or pandas on spark, casts to np.dtype("float64") and round to scale

    Parameters
    ----------
    precision : int
        precision of the decimal
    scale: int
        scale of the decimal

    Returns
    -------
    dataframe
        dataframe with given column(s) casted to the DecimalType(precision, scale) if
        dataframe is pypsark, otherwise cast to np.dtype("float64") and round to scale

    """

    spark_data_type = DecimalType
    pandas_type = np.dtype("float64")

    def __init__(self, precision: int = None, scale: int = 2):
        self.precision = precision
        self.scale = scale
        self.spark_type = DecimalType(precision=self.precision, scale=self.scale)

    def _cast_pyspark(self, df, column: str):
        df = df.withColumn(column, F.col(column).cast(self.spark_type))
        return df

    def _cast_pandas(self, df, column: str):
        df[column] = df[column].astype(self.pandas_type)
        df[column] = df[column].round(self.scale)

        return df

    def _cast_pandas_on_spark(self, df, column: str):
        df[column] = df[column].astype(self.pandas_type)
        df[column] = df[column].round(self.scale)
        return df
