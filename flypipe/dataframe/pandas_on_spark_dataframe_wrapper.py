import pyspark.pandas as ps
from numpy import dtype
from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.exceptions import DataFrameMissingColumns
from flypipe.schema.types import Boolean, Byte, Binary, Integer, Short, Long, Float, Double, String, Decimal, Type
from flypipe.utils import DataFrameType


#TODO: is there a better place to put this?
ps.set_option('compute.ops_on_diff_frames', True)


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
        try:
            return self.df[list(columns)]
        except KeyError:
            raise DataFrameMissingColumns(self.df.columns, list(columns))

    def _cast_column(self, column, flypipe_type, df_type):
        rows = self.df[column].notnull()

        self.df[column].loc[rows] = self.df[column].loc[rows].astype(df_type)

    def _cast_column_decimal(self, column, flypipe_type):
        rows = self.df[column].notnull()

        self.df[column].loc[rows] = self.df[column].loc[rows].astype(dtype('float64'))
        self.df[column].loc[rows] = self.df[column].loc[rows].round(flypipe_type.scale)

    def _cast_column_date(self, column, flypipe_type):
        return self._cast_column_date_or_timestamp(column, flypipe_type)

    def _cast_column_datetime(self, column, flypipe_type):
        return self._cast_column_date_or_timestamp(column, flypipe_type)

    def _cast_column_date_or_timestamp(self, column, flypipe_type):
        rows = self.df[column].notnull()
        self.df[column].loc[rows] = ps.to_datetime(
            self.df[column].loc[rows], format=flypipe_type.fmt).astype(dtype("datetime64[ns]"))
