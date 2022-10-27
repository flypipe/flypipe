import pytest
import pandas as pd
from flypipe.data_type import String, Integer
from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.dataframe.pandas_dataframe_wrapper import PandasDataFrameWrapper
from flypipe.dataframe.pandas_on_spark_dataframe_wrapper import PandasOnSparkDataFrameWrapper
from flypipe.dataframe.spark_dataframe_wrapper import SparkDataFrameWrapper
from flypipe.schema import Schema, Column
from tests.utils.spark import spark


class TestDataFrameWrapper:

    @pytest.mark.parametrize('df,expected_class', [
        (pd.DataFrame({'column': [1]}), PandasDataFrameWrapper),
        (spark.createDataFrame(schema=['column'], data=[[1]]), SparkDataFrameWrapper),
        (spark.createDataFrame(schema=['column'], data=[[1]]).to_pandas_on_spark(), PandasOnSparkDataFrameWrapper),
    ])
    def test_get_instance(self, df, expected_class):
        schema = Schema([Column('column', String(), '')])
        assert isinstance(DataFrameWrapper.get_instance(spark, df, schema), expected_class)

    @pytest.mark.parametrize('df', [
        pd.DataFrame({'col1': [1], 'col2': [2]}),
        spark.createDataFrame(schema=('col1', 'col2'), data=[[1, 2]]),
        spark.createDataFrame(schema=('col1', 'col2'), data=[[1, 2]]).to_pandas_on_spark(),
    ])
    def test_select_columns_out_of_place(self, df):
        """
        Ensure that DataFrameWrapper.select_columns does the selection operation out-of-place and returns a new
        dataframe wrapper, therefore the original dataframe wrapper should be untouched.
        """
        schema = Schema([
            Column('col1', Integer(), ''),
            Column('col2', Integer(), ''),
        ])
        df_wrapper = DataFrameWrapper.get_instance(spark, df, schema)
        df_wrapper2 = df_wrapper.select_columns('col1')
