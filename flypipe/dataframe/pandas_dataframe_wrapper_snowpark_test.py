"""Tests for PandasDataFrameWrapper - Snowpark functionality"""
import datetime
import os

import pandas as pd
import pytest

from flypipe import node
from flypipe.dataframe.pandas_dataframe_wrapper import PandasDataFrameWrapper
from flypipe.schema import Column, Schema
from flypipe.schema.types import (
    Boolean,
    Date,
    DateTime,
    Integer,
    Float,
    String,
)


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") != "SNOWFLAKE",
    reason="Snowpark tests require RUN_MODE=SNOWFLAKE",
)
class TestPandasDataFrameWrapperSnowpark:
    """Tests for PandasDataFrameWrapper - Snowpark functionality"""

    def test_get_column_flypipe_type(self, snowflake_session):
        """Test that get_column_flypipe_type correctly identifies column types"""
        df = pd.DataFrame(
            {
                "c1": [True],
                "c2": [1],
                "c3": [1.1],
                "c4": "bla",
                "c5": datetime.date(2022, 11, 4),
                "c6": datetime.datetime(2022, 11, 4, 12, 0, 0),
            }
        )
        df_wrapper = PandasDataFrameWrapper.get_instance(snowflake_session, df)
        assert isinstance(df_wrapper.get_column_flypipe_type("c1"), Boolean)
        assert isinstance(df_wrapper.get_column_flypipe_type("c2"), Integer)
        assert isinstance(df_wrapper.get_column_flypipe_type("c3"), Float)
        assert isinstance(df_wrapper.get_column_flypipe_type("c4"), String)
        assert isinstance(df_wrapper.get_column_flypipe_type("c5"), Date)
        assert isinstance(df_wrapper.get_column_flypipe_type("c6"), DateTime)

    def test_empty_dataframe(self, snowflake_session):
        """Test that empty DataFrames are handled correctly"""
        @node(
            type="pandas",
            output=Schema([Column("c1", Boolean())]),
        )
        def t1():
            return pd.DataFrame(columns=["c1"])

        t1.run(snowflake_session)

