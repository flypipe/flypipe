import pandas as pd
import pytest

from flypipe import node
from flypipe.converter.dataframe import DataFrameConverter
from flypipe.schema import Column, Schema
from flypipe.schema.types import Boolean
from flypipe.utils import assert_dataframes_equals, DataFrameType


@pytest.fixture(scope="function")
def spark():
    from flypipe.tests.spark import spark

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

    @pytest.mark.parametrize(
        "node_type",
        ["pyspark", "pandas_on_spark"],
    )
    def test_empty_dataframe_to_spark(self, spark, caplog, node_type):
        @node(
            type="pandas",
            output=Schema([Column("c1", Boolean())]),
        )
        def t1():
            return pd.DataFrame(columns=["c1"])

        @node(
            type=node_type,
            dependencies=[t1.alias("df_t1")],
            output=Schema([Column("c1", Boolean())]),
        )
        def t2(df_t1):
            return df_t1

        t2.run(spark, parallel=False)
        warning_text = (
            "pyspark.errors.exceptions.base.PySparkValueError: "
            "[CANNOT_INFER_EMPTY_SCHEMA] Can not infer schema from empty Pandas Dataframe to "
            "Pyspark Dataframe => Creating empty dataset with StringType() for all columns"
        )
        assert warning_text in str(caplog.text)
