from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.exceptions import SelectionNotFoundInDataFrame
from flypipe.utils import DataFrameType



class SparkDataFrameWrapper(DataFrameWrapper):
    TYPE = DataFrameType.PYSPARK

    def _select_columns(self, columns):

        df_cols = [col for col, _ in self.df.dtypes]

        if not set(columns).issubset(set(df_cols)):
            raise SelectionNotFoundInDataFrame(df_cols, columns)

        return self.df.select(list(columns))
