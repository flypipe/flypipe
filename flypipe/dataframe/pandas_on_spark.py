from flypipe.dataframe.dataframe import DataFrame
from flypipe.utils import DataFrameType


class PandasOnSparkDataFrame(DataFrame):
    TYPE = DataFrameType.PANDAS_ON_SPARK

    def select_columns(self, *columns):
        return PandasOnSparkDataFrame(self.spark, self.df[list(columns)], self.schema)
