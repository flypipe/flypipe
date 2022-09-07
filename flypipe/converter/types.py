from typing import Union
import numpy as np

from flypipe.exceptions import ErrorColumnNotInDataframe
from flypipe.utils import dataframe_type, DataFrameType, get_schema
import pyspark.sql.functions as F
import pyspark.pandas as ps
import pandas as pd
from pyspark.pandas.typedef import pandas_on_spark_type, spark_type_to_pandas_dtype
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# TODO: document


class Type:
    """Casts dataframe columns.
    When converting Spark dataframe to Pandas (on spark), some datatypes is converted as object,
    for example Spark Dataframe with a column DateType is converted to object.
    This class converts the the dataframe to the approppriate data type accordingly to
    pandas_on_spark_type and spark_type_to_pandas_dtype
    """

    def __init__(self):
        self.pandas_type, self.spark_type = pandas_on_spark_type(
            spark_type_to_pandas_dtype(self.spark_data_type())
        )

    def __repr__(self):
        return f"pandas_type {self.pandas_type}, spark_type: {self.spark_type}"

    @property
    def spark_data_type(self):
        raise NotImplementedError

    def columns(self, df, column: Union[str, list]):
        """Receives a str and return a list[str] or
        if receives a list, returns the list

        Parameters
        ----------
        df : dataframe
            dataframe in wich the column(s) to be casted

        column : str or list
            column(s) to casted

        Returns
        -------
        list
            list of columns

        Raises
        ------
        ErrorColumnNotInDataframe
            if column(s) provided to be cast do not exist in the dataframe
        """
        columns_to_cast = column if isinstance(column, list) else [column]
        non_existing_columns = list(set(columns_to_cast) - set(df.columns))
        if non_existing_columns:
            raise ErrorColumnNotInDataframe(non_existing_columns)

        return columns_to_cast

    def cast(self, df, column: Union[str, list]):
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
        columns_to_cast = self.columns(df, column)

        schema = get_schema(df, columns_to_cast)
        if dataframe_type(df) == DataFrameType.PYSPARK:

            columns_to_cast = {
                column: data_type
                for column, data_type in schema.items()
                if data_type != self.spark_type and column in columns_to_cast
            }

            for col in columns_to_cast.keys():
                df = df.withColumn(col, F.col(col).cast(self.spark_type))

        else:
            columns_to_cast = {
                column: data_type
                for column, data_type in schema.items()
                if data_type != self.pandas_type and column in columns_to_cast
            }

            if columns_to_cast:
                columns_to_cast = list(columns_to_cast.keys())
                df[columns_to_cast] = df[columns_to_cast].astype(self.pandas_type)

        return df


class Boolean(Type):
    """Casts dataframe to boolean"""

    spark_data_type = BooleanType


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


class Decimal(Type):
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

    def __init__(self, precision: int = None, scale: int = 2):
        self.precision = precision
        self.scale = scale

    def cast(self, df, column: Union[str, list]):
        """Casts dataframe to approppriate Decimal
        If dataframe is pyspark, casts to DecimalType(precision, scale)
        If dataframe is pandas or pandas on spark, casts to np.dtype("float64") and round to scale

        Parameters
        ----------
        df : dataframe
            dataframe to have column(s) casted
        column: str or list
            column(s) to be casted

        Returns
        -------
        dataframe
            dataframe with given column(s) casted to the DecimalType(precision, scale) if
            dataframe is pypsark, otherwise cast to np.dtype("float64") and round to scale

        """
        columns_to_cast = self.columns(df, column)

        schema = get_schema(df, columns_to_cast)
        if dataframe_type(df) == DataFrameType.PYSPARK:

            columns_to_cast = {
                column: data_type
                for column, data_type in schema.items()
                if data_type != DecimalType and column in columns_to_cast
            }

            for col in columns_to_cast.keys():
                df = df.withColumn(
                    col,
                    F.col(col).cast(
                        DecimalType(precision=self.precision, scale=self.scale)
                    ),
                )

        else:
            columns_to_cast = {
                column: data_type
                for column, data_type in schema.items()
                if data_type != np.dtype("float64") and column in columns_to_cast
            }
            columns_to_cast = list(columns_to_cast.keys())
            if columns_to_cast:
                df[columns_to_cast] = df[columns_to_cast].astype(np.dtype("float64"))
                df[columns_to_cast] = df[columns_to_cast].round(self.scale)

        return df
