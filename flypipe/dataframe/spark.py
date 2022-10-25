from flypipe.dataframe.dataframe import DataFrame
from flypipe.utils import DataFrameType


class SparkDataFrame(DataFrame):
    TYPE = DataFrameType.PYSPARK

    def select_columns(self, *columns):
        return SparkDataFrame(self.spark, self.df.select(*columns), self.schema)
