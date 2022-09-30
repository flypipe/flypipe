import numpy as np
import pyspark.sql.functions as F
from numpy import dtype
from pyspark.pandas.typedef import pandas_on_spark_type, spark_type_to_pandas_dtype
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    DoubleType,
    IntegerType,
    ShortType,
    LongType,
    ByteType,
    BinaryType,
    StringType,
)

from flypipe.dataframe_wrapper import DataframeWrapper
from flypipe.exceptions import ErrorColumnNotInDataframe
from flypipe.utils import dataframe_type, DataFrameType, get_schema


class Type:
    """Casts dataframe columns.
    When converting Spark dataframe to Pandas (on spark), some datatypes is converted as object,
    for example Spark Dataframe with a column DateType is converted to object.
    This class converts the the dataframe to the approppriate data type accordingly to
    pandas_on_spark_type and spark_type_to_pandas_dtype
    """

    spark_type = None
    pandas_type = None

    def __init__(self):
        self._pandas_type = None
        self._spark_type = None

    def __repr__(self):
        return f"pandas_type {self.pandas_type}, spark_type: {self.spark_type}"

    @property
    def spark_data_type(self):
        raise NotImplementedError

    def cast(self, df, df_type: DataFrameType, column: str):
        """Receives a str and return a list[str] or
        if receives a list, returns the list

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
        if df_type == DataFrameType.PYSPARK:
            df = self._cast_pyspark(df, column)
        elif df_type == DataFrameType.PANDAS:
            df = self._cast_pandas(df, column)
        elif df_type == DataFrameType.PANDAS_ON_SPARK:
            df = self._cast_pandas_on_spark(df, column)
        return df

    def _cast_pyspark(self, df, column: str):
        df = df.withColumn(column, F.col(column).cast(self.spark_type))
        return df

    def _cast_pandas(self, df, column: str):
        df[column] = df[column].astype(self.pandas_type)
        return df

    def _cast_pandas_on_spark(self, df, column: str):
        df[column] = df[column].astype(self.pandas_type)
        return df


class Boolean(Type):
    """Casts dataframe to boolean"""

    spark_data_type = BooleanType
    pandas_data_type = np.bool_


class Byte(Type):
    """Casts dataframe to byte"""

    spark_data_type = ByteType


class Binary(Type):
    """Casts dataframe to binary"""

    spark_data_type = BinaryType
    pandas_type = np.bytes_
    spark_type = BinaryType()

    def __init__(self):
        pass


class Integer(Type):
    """Casts dataframe to integer"""

    spark_data_type = IntegerType


class Short(Type):
    """Casts dataframe to short"""

    spark_data_type = ShortType


class Long(Type):
    """Casts dataframe to long"""

    spark_data_type = LongType


class Float(Type):
    """Casts dataframe to float"""

    spark_data_type = FloatType


class Double(Type):
    """Casts dataframe to double"""

    spark_data_type = DoubleType


class String(Type):
    """Casts dataframe to string"""

    spark_data_type = StringType
    pandas_type = dtype("<U0")
    spark_type = StringType()

    def __init__(self):
        pass
