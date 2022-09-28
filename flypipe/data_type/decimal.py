from typing import Union

import numpy as np
import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType

from flypipe.data_type.type import Type
from flypipe.utils import dataframe_type, DataFrameType, get_schema


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
    pandas_type = np.dtype("<M8[ns]")

    def __init__(self, precision: int = None, scale: int = 2):
        self.precision = precision
        self.scale = scale
        self.spark_type = DecimalType(precision=self.precision, scale=self.scale)

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
                    F.col(col).cast(self.spark_type),
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
