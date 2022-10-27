from flypipe.dataframe.dataframe import DataFrameWrapper
from flypipe.utils import DataFrameType


class PandasDataFrame(DataFrameWrapper):
    TYPE = DataFrameType.PANDAS

    def _select_columns(self, columns):
        return self.df[list(columns)]
