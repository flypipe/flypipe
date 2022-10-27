from flypipe.dataframe.dataframe import DataFrameWrapper
from flypipe.utils import DataFrameType


class PandasOnSparkDataFrame(DataFrameWrapper):
    TYPE = DataFrameType.PANDAS_ON_SPARK

    def _select_columns(self, columns):
        return self.df[list(columns)]
