from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.utils import DataFrameType


class SparkDataFrameWrapper(DataFrameWrapper):
    TYPE = DataFrameType.PYSPARK

    def _select_columns(self, columns):
        return self.df.select(list(columns))
