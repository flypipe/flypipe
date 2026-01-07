"""Tests for DataFrameConverter - Core (Pandas-only) functionality"""

import os
import pandas as pd
import pytest

from flypipe.converter.dataframe import DataFrameConverter
from flypipe.utils import assert_dataframes_equals, DataFrameType


@pytest.fixture(scope="function")
def pandas_df():
    return pd.DataFrame(data={"col1": [1, 2, 3], "col2": ["1a", "2a", "3a"]})


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") != "CORE",
    reason="Core tests require RUN_MODE=CORE",
)
class TestDataFrameConverterCore:
    """Tests on DataFrameConverter - Core (Pandas-only) functionality"""

    def test_convert_pandas_to_pandas(self, pandas_df):
        """Test that Pandas to Pandas conversion returns the same DataFrame"""
        df = DataFrameConverter().convert(pandas_df, DataFrameType.PANDAS)
        assert_dataframes_equals(df, pandas_df)
