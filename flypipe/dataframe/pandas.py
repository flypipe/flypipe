from flypipe.dataframe.dataframe import DataFrameWrapper
from flypipe.utils import DataFrameType


class PandasDataFrame(DataFrameWrapper):
    TYPE = DataFrameType.PANDAS

    def select_columns(self, *columns):
        return PandasDataFrame(self.spark, self.df[list(columns)], self.schema)
