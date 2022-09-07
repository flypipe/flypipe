from numpy import int8

from flypipe.converter.dataframe import DataFrameConverter
from flypipe.utils import DataFrameType, dataframe_type


class SchemaConverter:
    def read_schema(self, df):
        if dataframe_type(df) == DataFrameType.PYSPARK:
            return {col: df.schema[col].dataType.__class__() for col in df.columns}
        else:
            return {col: type_ for col, type_ in df.dtypes.items()}

    def convert(self, fom_dtypes, df):
        to_type = dataframe_type(df)
