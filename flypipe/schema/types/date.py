import numpy as np
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import StringType, DateType, TimestampType
from flypipe.schema.types.datetime import Datetime
from flypipe.schema.types.string import String
from flypipe.schema.types.type import Type
from flypipe.utils import dataframe_type, DataFrameType
from pandas.api.types import is_datetime64_any_dtype


class Date(Type):

    NAME = 'date'
    ALLOWED_TYPE_CASTS = [
        String.NAME,
        Datetime.NAME
    ]

    def __init__(self, date_format='%Y-%m-%d'):
        self.date_format = date_format

    def validate(self, df, column_name):
        if dataframe_type(df) == DataFrameType.PYSPARK:
            if df.schema[column_name].dataType not in (DateType(), TimestampType()):
                self.raise_validation_error(column_name, df.dtypes[column_name])
        elif dataframe_type(df) == DataFrameType.PANDAS:
            if df.dtypes[column_name] != np.object and not is_datetime64_any_dtype(df[column_name]):
                self.raise_validation_error(column_name, df.dtypes[column_name])

    def cast_string(self, df, column_name, **kwargs):
        if dataframe_type(df) == DataFrameType.PYSPARK:
            return df.withColumn(column_name, getattr(df, column_name).cast(StringType()))
        elif dataframe_type(df) == DataFrameType.PANDAS:
            df[column_name] = df[column_name].astype('string')

    def cast_datetime(self, df, column_name, **kwargs):
        return df.withColumn(column_name, to_timestamp(column_name))
