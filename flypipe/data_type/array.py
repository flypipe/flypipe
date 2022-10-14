import warnings

import numpy as np
import pyspark.sql.functions as F
from numpy import dtype
from pyspark.sql.types import ArrayType

from flypipe.data_type.type import Type


class ArrayContentCast(UserWarning):
    pass


class Array(Type):
    """Casts dataframe to array

    Attributes
    ----------
    type: flypipe.data_type.Type
        Defines the type of the array
    """

    spark_type = ArrayType
    pandas_type = dtype("O")

    def __init__(self, type):
        assert (
            type is not None
        ), "Error: please define the type of the array, ie. Array(Integer())"

        warnings.warn(
            ArrayContentCast(
                "Make sure the content of the array has been casted to the proper type"
            )
        )

        self.type = type
        self.spark_type = ArrayType(self.type.spark_type)

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self.type)})"

    def _cast_pyspark(self, df, column: str):
        df = df.withColumn(column, F.col(column).cast(self.spark_type))
        return df

    def _cast_pandas(self, df, column: str):
        df[column] = df[column].astype(self.pandas_type)
        return df

    def _cast_pandas_on_spark(self, df, column: str):
        df[column] = df[column].astype(np.ndarray)
        return df
