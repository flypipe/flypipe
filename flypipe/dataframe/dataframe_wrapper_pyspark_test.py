"""Tests for DataFrameWrapper - PySpark functionality"""
import os
import pandas as pd
import pytest

from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.dataframe.pandas_dataframe_wrapper import PandasDataFrameWrapper
from flypipe.dataframe.pandas_on_spark_dataframe_wrapper import (
    PandasOnSparkDataFrameWrapper,
)
from flypipe.dataframe.spark_dataframe_wrapper import SparkDataFrameWrapper


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") not in ["SPARK", "SPARK_CONNECT"],
    reason="PySpark tests require RUN_MODE=SPARK or SPARK_CONNECT",
)
class TestDataFrameWrapperPySpark:
    """Tests for DataFrameWrapper - PySpark functionality"""

    @pytest.mark.parametrize(
        "data,type,expected_class",
        [
            (pd.DataFrame({"column": [1]}), "pandas", PandasDataFrameWrapper),
            (
                {"schema": ["column"], "data": [[1]]},
                "spark",
                SparkDataFrameWrapper,
            ),
            (
                {"schema": ["column"], "data": [[1]]},
                "pandas_api",
                PandasOnSparkDataFrameWrapper,
            ),
        ],
    )
    def test_get_instance(self, data, type, expected_class, spark):
        """Test that get_instance returns the correct wrapper class for different DataFrame types"""
        if type == "pandas":
            df = data
        else:
            df = spark.createDataFrame(**data)
            if type == "pandas_api":
                df = df.pandas_api()
        assert isinstance(DataFrameWrapper.get_instance(spark, df), expected_class)

    @pytest.mark.parametrize(
        "data,type",
        [
            (pd.DataFrame({"col1": [1], "col2": [2]}), "pandas"),
            ({"schema": ["col1", "col2"], "data": [[1, 2]]}, "spark"),
            ({"schema": ["col1", "col2"], "data": [[1, 2]]}, "pandas_api"),
        ],
    )
    def test_select_columns_out_of_place(self, spark, data, type):
        """
        Ensure that DataFrameWrapper.select_columns does the selection operation out-of-place and returns a new
        dataframe wrapper, therefore the original dataframe wrapper should be untouched.
        """
        if type == "pandas":
            df = data
        else:
            df = spark.createDataFrame(**data)
            if type == "pandas_api":
                df = df.pandas_api()
        
        df_wrapper = DataFrameWrapper.get_instance(spark, df)
        df_wrapper2 = df_wrapper.select_columns("col1")
        
        # Assert it returns a new instance
        assert df_wrapper2 is not df_wrapper
        
        # Assert the new wrapper has only the selected column
        assert list(df_wrapper2.df.columns) == ["col1"]
        
        # Assert the original wrapper is untouched (still has all columns)
        assert list(df_wrapper.df.columns) == ["col1", "col2"]

