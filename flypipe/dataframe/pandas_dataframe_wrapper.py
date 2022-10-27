from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.utils import DataFrameType


class PandasDataFrameWrapper(DataFrameWrapper):
    TYPE = DataFrameType.PANDAS

    def _select_columns(self, columns):
        return self.df[list(columns)]
