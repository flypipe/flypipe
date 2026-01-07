"""Tests for DataFrameWrapper - Snowpark functionality"""

import os
import pandas as pd
import pytest

from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.dataframe.snowpark_dataframe_wrapper import SnowparkDataFrameWrapper


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") != "SNOWFLAKE",
    reason="Snowpark tests require RUN_MODE=SNOWFLAKE",
)
class TestDataFrameWrapperSnowpark:
    """Tests for DataFrameWrapper - Snowpark functionality"""

    def test_get_instance(self, snowflake_session):
        """Test that get_instance returns SnowparkDataFrameWrapper for Snowpark DataFrames"""
        df = snowflake_session.create_dataframe(pd.DataFrame({"column": [1]}))
        assert isinstance(
            DataFrameWrapper.get_instance(snowflake_session, df),
            SnowparkDataFrameWrapper,
        )

    def test_select_columns_out_of_place(self, snowflake_session):
        """
        Ensure that DataFrameWrapper.select_columns does the selection operation out-of-place for Snowpark DataFrames.
        """
        df = snowflake_session.create_dataframe(
            pd.DataFrame({"col1": [1], "col2": [2]})
        )

        df_wrapper = DataFrameWrapper.get_instance(snowflake_session, df)
        # Note: Snowflake quotes column names when created from pandas
        df_wrapper2 = df_wrapper.select_columns('"col1"')

        # Assert it returns a new instance
        assert df_wrapper2 is not df_wrapper

        # Assert the new wrapper has only the selected column
        assert list(df_wrapper2.df.columns) == ['"col1"']

        # Assert the original wrapper is untouched (still has all columns)
        assert list(df_wrapper.df.columns) == ['"col1"', '"col2"']
