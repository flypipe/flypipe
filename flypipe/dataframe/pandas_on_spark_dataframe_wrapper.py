import numpy as np
import pandas as pd
from numpy import dtype
from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.schema.types import Boolean, Byte, Binary, Integer, Short, Long, Float, Double, String, Decimal, Type
from flypipe.utils import DataFrameType


class PandasOnSparkDataFrameWrapper(DataFrameWrapper):
    DF_TYPE = DataFrameType.PANDAS_ON_SPARK
    _TYPE_MAP = {
        Boolean.key(): dtype('bool'),
        Byte.key(): dtype('int8'),
        Binary.key(): dtype('S'),
        Integer.key(): dtype('int32'),
        Short.key(): dtype('int16'),
        Long.key(): dtype('int64'),
        Float.key(): dtype('float32'),
        Double.key(): dtype('float64'),
        String.key(): dtype("<U0"),
    }

    def _select_columns(self, columns):
        return self.df[list(columns)]

    def _get_rows_for_cast(self, column, flypipe_type):
        rows = self.df[column].notnull()
        invalid_value_indexes = np.flatnonzero(
            ~self.df[column].loc[rows].apply(lambda row: flypipe_type.is_valid_value(row)))
        if invalid_value_indexes.any():
            raise ValueError(
                f'Invalid type {flypipe_type.name} for column {column}, found incompatible row value '
                f'"{self.df[column].loc[invalid_value_indexes[0]]}"')
        return rows

    def _cast_column(self, column, flypipe_type, df_type):
        rows = self._get_rows_for_cast(column, flypipe_type)

        self.df[column].loc[rows] = self.df[column].loc[rows].astype(df_type)

    def _cast_column_decimal(self, column, flypipe_type):
        rows = self._get_rows_for_cast(column, flypipe_type)

        self.df[column].loc[rows] = self.df[column].loc[rows].astype(dtype('float64'))
        self.df[column].loc[rows] = self.df[column].loc[rows].round(flypipe_type.scale)

    def _cast_column_date(self, column, flypipe_type):
        return self._cast_column_date_or_timestamp(column, flypipe_type)

    def _cast_column_datetime(self, column, flypipe_type):
        return self._cast_column_date_or_timestamp(column, flypipe_type)

    def _cast_column_date_or_timestamp(self, column, flypipe_type):
        rows = self._get_rows_for_cast(column, flypipe_type)
        self.df[column].loc[rows] = pd.to_datetime(
            self.df[column].loc[rows], format=flypipe_type.fmt).astype(dtype("datetime64[ns]"))
