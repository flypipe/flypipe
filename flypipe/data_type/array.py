import warnings

import numpy as np
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

    spark_data_type = ArrayType
    pandas_type = np.ndarray

    def __init__(self, type):
        assert (
            type is not None
        ), "Error: please define the type of the array, ie. Array(Integer())"

        warnings.warn(
            ArrayContentCast(
                "Make sure the content of the array has been casted to the proper type"
            )
        )

        array_pandas_type, array_spark_type = type.pandas_type, type.spark_type
        self.spark_type = ArrayType(array_spark_type)
