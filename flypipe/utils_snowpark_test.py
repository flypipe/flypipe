import os
import pandas as pd
import pytest

from flypipe.exceptions import (
    DataframeDifferentDataError,
    DataframeSchemasDoNotMatchError,
    DataframeTypeNotSupportedError,
)
from flypipe.utils import assert_dataframes_equals, DataFrameType, dataframe_type


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") != "SNOWFLAKE",
    reason="Snowpark tests require RUN_MODE=SNOWFLAKE",
)
class TestUtilsSnowpark:
    """Tests on Utils - Snowpark"""

    def test_assert_dataframes_equals(self, snowflake_session):
        """Test assert_dataframes_equals with Snowpark DataFrames
        
        Note: Full schema validation is not yet implemented for Snowpark DataFrames.
        This test verifies basic equality checking only.
        """
        df1 = snowflake_session.create_dataframe(
            pd.DataFrame(data={"col1": [1, 2], "col2": ["1a", "2a"]})
        )
        df2 = snowflake_session.create_dataframe(
            pd.DataFrame(data={"col1": [1, 2], "col2": ["1a", "2a"]})
        )
        # Basic equality check should work
        assert_dataframes_equals(df1, df2)

        # Test with unsupported type
        with pytest.raises(DataframeTypeNotSupportedError):
            assert_dataframes_equals(df1, 2)

    def test_dataframe_type(self, snowflake_session):
        """Test dataframe_type with Snowpark DataFrames"""
        df = pd.DataFrame(data={"col1": [1, 2, 3], "col2": ["1a", "2a", "3a"]})
        assert dataframe_type(df) == DataFrameType.PANDAS

        df = snowflake_session.create_dataframe(df)
        assert dataframe_type(df) == DataFrameType.SNOWPARK

        with pytest.raises(DataframeTypeNotSupportedError):
            dataframe_type(1)

