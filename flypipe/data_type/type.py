import pyspark.sql.functions as F
from numpy import dtype
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

from flypipe.exceptions import ErrorColumnNotInDataframe
from flypipe.utils import DataFrameType


class Type:
    """Casts dataframe columns.
    When converting Spark dataframe to Pandas (on spark), some datatypes is converted as object,
    for example Spark Dataframe with a column DateType is converted to object.
    This class converts the the dataframe to the approppriate data type accordingly to
    pandas_on_spark_type and spark_type_to_pandas_dtype
    """

    spark_type = None
    pandas_type = None

    def __repr__(self):
        return f"pandas_type {self.pandas_type}, spark_type: {self.spark_type}"


    def cast(self, df, df_type: DataFrameType, column: str):
        """Receives a str and return a list[str] or
        if receives a list, returns the list

        Parameters
        ----------
        df : dataframe
            dataframe to have column(s) casted
        df_type : DataFrameType
            type of the dataframe
        column: str or list
            column(s) to be casted

        Returns
        -------
        dataframe
            dataframe with given column(s) casted to the DataType defined by the child in spark_data_type
        """

        if column not in df.columns:
            raise ErrorColumnNotInDataframe(column)

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

    spark_type = BooleanType()
    pandas_type = dtype("bool")


class Byte(Type):
    """Casts dataframe to byte"""

    spark_type = ByteType()
    pandas_type = dtype("int8")

class Binary(Type):
    """Casts dataframe to binary"""

    spark_type = BinaryType()
    pandas_type = dtype("S")


class Integer(Type):
    """Casts dataframe to integer"""

    spark_type = IntegerType()
    pandas_type = dtype("int32")

class Short(Type):
    """Casts dataframe to short"""

    spark_type = ShortType()
    pandas_type = dtype("int16")

class Long(Type):
    """Casts dataframe to long"""

    spark_type = LongType()
    pandas_type = dtype("int64")

class Float(Type):
    """Casts dataframe to float"""

    spark_type = FloatType()
    pandas_type = dtype("float32")

class Double(Type):
    """Casts dataframe to double"""

    spark_type = DoubleType()
    pandas_type = dtype("float64")

class String(Type):
    """Casts dataframe to string"""

    spark_type = StringType()
    pandas_type = dtype("<U0")

    def __init__(self):
        pass
