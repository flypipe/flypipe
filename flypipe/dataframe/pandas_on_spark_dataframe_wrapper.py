from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.utils import DataFrameType


class PandasOnSparkDataFrameWrapper(DataFrameWrapper):
    TYPE = DataFrameType.PANDAS_ON_SPARK

    def _select_columns(self, columns):
        return self.df[list(columns)]
