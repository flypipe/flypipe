from flypipe.dataframe.dataframe import DataFrame
from flypipe.utils import DataFrameType


class PandasDataFrame(DataFrame):
    TYPE = DataFrameType.PANDAS

    def select_columns(self, *columns):
        return PandasDataFrame(self.spark, self.df[list(columns)], self.schema)
