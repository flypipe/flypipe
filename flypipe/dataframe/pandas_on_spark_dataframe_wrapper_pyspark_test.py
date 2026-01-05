"""Tests for PandasOnSparkDataFrameWrapper - PySpark functionality"""
import os
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql.types import (
    StructField,
    StructType,
    BooleanType,
)

from flypipe.dataframe.pandas_on_spark_dataframe_wrapper import PandasOnSparkDataFrameWrapper
from flypipe.exceptions import DataFrameMissingColumns
from flypipe.schema.types import (
    Boolean,
)


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") not in ["SPARK", "SPARK_CONNECT"],
    reason="PySpark tests require RUN_MODE=SPARK or SPARK_CONNECT",
)
class TestPandasOnSparkDataFrameWrapperPySpark:
    """Tests for PandasOnSparkDataFrameWrapper - PySpark functionality"""

    def test_select_column_1(self, spark):
        """Test selecting columns using positional arguments"""
        df = spark.createDataFrame(
            pd.DataFrame(
                {
                    "col1": [True, False],
                    "col2": ["Hello", "World"],
                    "col3": ["Banana", "Apple"],
                }
            )
        ).pandas_api()

        expected_df = pd.DataFrame(
            {
                "col1": [True, False],
                "col2": ["Hello", "World"],
            }
        )
        df_wrapper = PandasOnSparkDataFrameWrapper.get_instance(None, df)
        assert_frame_equal(
            df_wrapper.select_columns("col1", "col2").df.to_pandas(), expected_df
        )

    def test_select_column_2(self, spark):
        """Test selecting columns using a list"""
        df = spark.createDataFrame(
            pd.DataFrame(
                {
                    "col1": [True, False],
                    "col2": ["Hello", "World"],
                    "col3": ["Banana", "Apple"],
                }
            )
        ).pandas_api()

        expected_df = pd.DataFrame(
            {
                "col1": [True, False],
                "col2": ["Hello", "World"],
            }
        )
        df_wrapper = PandasOnSparkDataFrameWrapper.get_instance(None, df)
        assert_frame_equal(
            df_wrapper.select_columns(["col1", "col2"]).df.to_pandas(), expected_df
        )

    def test_select_column_missing_column(self, spark):
        """Test that selecting a missing column raises DataFrameMissingColumns"""
        df = spark.createDataFrame(
            pd.DataFrame(
                {
                    "col1": [True, False],
                    "col2": ["Hello", "World"],
                    "col3": ["Banana", "Apple"],
                }
            )
        ).pandas_api()

        df_wrapper = PandasOnSparkDataFrameWrapper.get_instance(None, df)

        with pytest.raises(DataFrameMissingColumns):
            df_wrapper.select_columns(["col1", "col4"])

    def test_get_column_flypipe_type(self, spark):
        """Test that get_column_flypipe_type correctly identifies column types"""
        df = spark.createDataFrame(
            schema=StructType(
                [
                    StructField("c1", BooleanType()),
                ]
            ),
            data=[],
        ).pandas_api()
        df_wrapper = PandasOnSparkDataFrameWrapper.get_instance(spark, df)

        assert isinstance(df_wrapper.get_column_flypipe_type("c1"), Boolean)

