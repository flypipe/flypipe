from flypipe.dataframe.dataframe import DataFrameWrapper
from flypipe.utils import DataFrameType


class PandasOnSparkDataFrame(DataFrameWrapper):
    TYPE = DataFrameType.PANDAS_ON_SPARK

    def select_columns(self, *columns):
        return PandasOnSparkDataFrame(self.spark, self.df[list(columns)], self.schema)
