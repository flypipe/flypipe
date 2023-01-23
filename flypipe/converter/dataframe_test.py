import pandas as pd
import pytest

from flypipe.converter.dataframe import DataFrameConverter
from flypipe.utils import assert_dataframes_equals, DataFrameType


@pytest.fixture(scope="function")
def spark():
    from flypipe.tests.spark import spark  # pylint: disable=import-outside-toplevel

    return spark


@pytest.fixture(scope="function")
def pandas_df():
    return pd.DataFrame(data={"col1": [1, 2, 3], "col2": ["1a", "2a", "3a"]})


@pytest.fixture(scope="function")
def pyspark_df(spark, pandas_df):
    return spark.createDataFrame(pandas_df)


@pytest.fixture(scope="function")
def pandas_on_spark_df(pyspark_df):
    return pyspark_df.pandas_api()


class TestDataFrameConverter:
    """Tests on DataFrameConverts"""

    def test_convert_pandas_to_pandas(self, pandas_df):
        df = DataFrameConverter().convert(pandas_df, DataFrameType.PANDAS)
        assert_dataframes_equals(df, pandas_df)

    @pytest.mark.skip("Test succeeds locally but fails on GitHub")
    def test_convert_pandas_to_pandas_on_spark(
        self, spark, pandas_df, pandas_on_spark_df
    ):
        df = DataFrameConverter(spark).convert(pandas_df, DataFrameType.PANDAS_ON_SPARK)
        assert_dataframes_equals(df, pandas_on_spark_df)

    @pytest.mark.skip("Test succeeds locally but fails on GitHub")
    def test_convert_pandas_to_pyspark(self, spark, pandas_df, pyspark_df):
        df = DataFrameConverter(spark).convert(pandas_df, DataFrameType.PYSPARK)
        assert_dataframes_equals(df, pyspark_df)

    def test_convert_pandas_on_spark_to_pandas(
        self, spark, pandas_on_spark_df, pandas_df
    ):
        df = DataFrameConverter(spark).convert(pandas_on_spark_df, DataFrameType.PANDAS)
        assert_dataframes_equals(df, pandas_df)

    @pytest.mark.skip("Test succeeds locally but fails on GitHub")
    def test_convert_pandas_on_spark_to_pyspark(self, pandas_on_spark_df, pyspark_df):
        df = DataFrameConverter(spark).convert(
            pandas_on_spark_df, DataFrameType.PYSPARK
        )
        assert_dataframes_equals(df, pyspark_df)

    def test_convert_pyspark_to_pandas(self, spark, pyspark_df, pandas_df):
        df = DataFrameConverter(spark).convert(pyspark_df, DataFrameType.PANDAS)
        assert_dataframes_equals(df, pandas_df)

    @pytest.mark.skip("Test succeeds locally but fails on GitHub")
    def test_convert_pyspark_to_pandas_on_spark(self, pyspark_df, pandas_on_spark_df):
        df = DataFrameConverter(spark).convert(
            pyspark_df, DataFrameType.PANDAS_ON_SPARK
        )
        assert_dataframes_equals(df, pandas_on_spark_df)
