from flypipe.dataframe.dataframe import DataFrameWrapper
from flypipe.utils import DataFrameType


class SparkDataFrame(DataFrameWrapper):
    TYPE = DataFrameType.PYSPARK

    def _select_columns(self, columns):
        self.df.select(list(columns))
