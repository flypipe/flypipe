from flypipe.dataframe.dataframe import DataFrameWrapper
from flypipe.utils import DataFrameType


class SparkDataFrame(DataFrameWrapper):
    TYPE = DataFrameType.PYSPARK

    def select_columns(self, *columns):
        return SparkDataFrame(self.spark, self.df.select(*columns), self.schema)
