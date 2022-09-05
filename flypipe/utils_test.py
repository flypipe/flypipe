import pandas as pd
import pytest

from flypipe import DataframesSchemasDoNotMatch, \
    DataframesDifferentData, DataframesNotEquals, DataFrameTypeNotSupported

from flypipe import assert_dataframes_equals, DataFrameType, dataframe_type


@pytest.fixture
def spark():
    from tests.utils.spark import spark
    return spark


class TestUtils:

    def test_assert_dataframes_equals(self, spark):
        df1 = spark.createDataFrame(pd.DataFrame(data={"col1": [1, 2], "col2": ["1a", "2a"]}))
        df2 = spark.createDataFrame(pd.DataFrame(data={"col1": [1, 2], "col2": ["1a", "2a"]}))
        assert_dataframes_equals(df1, df2)

        df2 = spark.createDataFrame(pd.DataFrame(data={"col1": [1, 2], "col2": [1, 2]}))
        with pytest.raises(DataframesSchemasDoNotMatch):
            assert_dataframes_equals(df1, df2)

        df2 = spark.createDataFrame(pd.DataFrame(data={"col1": [1, 2, 3], "col2": ["1a", "2a", "3a"]}))
        with pytest.raises(DataframesDifferentData):
            assert_dataframes_equals(df1, df2)

        df2 = spark.createDataFrame(pd.DataFrame(data={"col1": [1, 2, 3], "col2": ["1a", "2a", "3a"], "col3": [1,2,3]}))
        with pytest.raises(DataframesSchemasDoNotMatch):
            assert_dataframes_equals(df1, df2)

        with pytest.raises(DataframesNotEquals):
            assert_dataframes_equals(df1, df2)

        with pytest.raises(DataFrameTypeNotSupported):
            assert_dataframes_equals(df1, 2)

    def test_dataframe_type(self, spark):
        df = pd.DataFrame(data={"col1": [1, 2, 3], "col2": ["1a", "2a", "3a"]})
        assert dataframe_type(df) == DataFrameType.PANDAS

        df = spark.createDataFrame(df)
        assert dataframe_type(df) == DataFrameType.PYSPARK

        df = df.to_pandas_on_spark()
        assert dataframe_type(df) == DataFrameType.PANDAS_ON_SPARK

        with pytest.raises(DataFrameTypeNotSupported):
            dataframe_type(1)
