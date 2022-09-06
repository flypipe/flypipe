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
    def __init__(self):
        self.pandas_type, self.spark_type = pandas_on_spark_type(
            spark_type_to_pandas_dtype(self.SPARK_TYPE())
        )

    def __repr__(self):
        return f"pandas_type {self.pandas_type}, spark_type: {self.spark_type}"

    def columns(self, column):
        return column if isinstance(column, list) else [column]

    def cast(self, df, column: Union[str, list]):

        columns_to_cast = self.columns(column)
        non_existing_columns = set(columns_to_cast) - set(df.columns)
        if non_existing_columns:
            raise ErrorColumnNotInDataframe(non_existing_columns)

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
            columns_to_cast = list(columns_to_cast.keys())
            df[columns_to_cast] = df[columns_to_cast].astype(self.pandas_type)

        return df


class Boolean(Type):
    SPARK_TYPE = BooleanType


class Date(Type):
    SPARK_TYPE = DateType

    def __init__(self, fmt: str = "%Y-%m-%d"):
        self.fmt = fmt
        self.pandas_type = np.dtype("<M8[ns]")
        self.spark_type = DateType()

    def cast(self, df, column: Union[str, list]):
        columns_to_cast = self.columns(column)
        non_existing_columns = set(columns_to_cast) - set(df.columns)
        if non_existing_columns:
            raise ErrorColumnNotInDataframe(non_existing_columns)

        schema = get_schema(df, columns_to_cast)

        if dataframe_type(df) == DataFrameType.PYSPARK:

            columns_to_cast = {
                column: data_type
                for column, data_type in schema.items()
                if data_type != self.spark_type and column in columns_to_cast
            }

            for column, data_type in columns_to_cast.items():
                if data_type == StringType():
                    df = df.withColumn(column, F.to_date(F.col(column), self.fmt))
                else:
                    df = df.withColumn(column, F.col(column).cast(self.spark_type))

        else:
            columns_to_cast = {
                column: data_type
                for column, data_type in schema.items()
                if data_type != self.pandas_type and column in columns_to_cast
            }

            for column, data_type in columns_to_cast.items():
                if data_type == np.dtype("object_"):
                    if dataframe_type(df) == DataFrameType.PANDAS_ON_SPARK:
                        df[column] = ps.to_datetime(df[column], format=self.fmt)
                    else:
                        df[column] = pd.to_datetime(df[column], format=self.fmt)

            columns_to_cast = list(columns_to_cast.keys())
            df[columns_to_cast] = df[columns_to_cast].astype(self.pandas_type)

        return df
