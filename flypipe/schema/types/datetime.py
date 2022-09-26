import numpy as np
from pandas.core.dtypes.common import is_datetime64_any_dtype
from pyspark.sql.functions import to_date
from pyspark.sql.types import StringType, DateType, TimestampType
from flypipe.schema.types.date import Date
from flypipe.schema.types.string import String
from flypipe.schema.types.type import Type
from flypipe.utils import dataframe_type, DataFrameType


class Datetime(Type):

    NAME = 'datetime'
    ALLOWED_TYPE_CASTS = [
        String.NAME,
        Date.NAME
    ]

    def validate(self, df, column_name):
        if dataframe_type(df) == DataFrameType.PYSPARK:
            if df.schema[column_name].dataType not in (DateType(), TimestampType()):
                self.raise_validation_error(column_name, df.dtypes[column_name])
        elif dataframe_type(df) == DataFrameType.PANDAS:
            if df.dtypes[column_name] != np.object and not is_datetime64_any_dtype(df[column_name]):
                self.raise_validation_error(column_name, df.dtypes[column_name])

    @classmethod
    def cast_string(cls, df, column_name, **kwargs):
        return df.withColumn(column_name, getattr(df, column_name).cast(StringType()))

    @classmethod
    def cast_date(cls, df, column_name, **kwargs):
        return df.withColumn(column_name, to_date(column_name))
