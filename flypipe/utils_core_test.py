import os
import pandas as pd
import pytest

from flypipe.exceptions import DataframeTypeNotSupportedError
from flypipe.utils import DataFrameType, dataframe_type


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") != "CORE",
    reason="Core tests require RUN_MODE=CORE",
)
class TestUtilsCore:
    """Tests on Utils - Core functionality"""

    def test_dataframe_type_pandas(self):
        """Test dataframe_type with Pandas DataFrame"""
        df = pd.DataFrame(data={"col1": [1, 2, 3], "col2": ["1a", "2a", "3a"]})
        assert dataframe_type(df) == DataFrameType.PANDAS

    def test_dataframe_type_unsupported(self):
        """Test dataframe_type raises exception for unsupported types"""
        with pytest.raises(DataframeTypeNotSupportedError):
            dataframe_type(1)

        with pytest.raises(DataframeTypeNotSupportedError):
            dataframe_type("not a dataframe")

        with pytest.raises(DataframeTypeNotSupportedError):
            dataframe_type(None)
