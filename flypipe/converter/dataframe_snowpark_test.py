"""Tests for DataFrameConverter - Snowpark functionality"""
import os
import pandas as pd
import pytest

from flypipe.converter.dataframe import DataFrameConverter, UnsupportedConversionError
from flypipe.utils import assert_dataframes_equals, DataFrameType


# Check if PySpark is available for cross-backend conversion tests
try:
    import pyspark
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") != "SNOWFLAKE",
    reason="Snowpark tests require RUN_MODE=SNOWFLAKE",
)
class TestDataFrameConverterSnowpark:
    """Tests on DataFrameConverter - Snowpark functionality"""

    @pytest.fixture(scope="function")
    def pandas_df(self):
        return pd.DataFrame(data={"col1": [1, 2, 3], "col2": ["1a", "2a", "3a"]})

    # ========================================
    # Pandas to Snowpark Conversions
    # ========================================

    def test_convert_pandas_to_snowpark(self, snowflake_session, pandas_df):
        """Test converting Pandas DataFrame to Snowpark DataFrame"""
        df = DataFrameConverter(snowflake_session).convert(pandas_df, DataFrameType.SNOWPARK)
        # Compare by converting back to pandas
        result_pandas = df.to_pandas()
        assert_dataframes_equals(result_pandas, pandas_df)

    # ========================================
    # Snowpark to Pandas Conversions
    # ========================================

    def test_convert_snowpark_to_pandas(self, snowflake_session, pandas_df):
        """Test converting Snowpark DataFrame to Pandas DataFrame"""
        snowpark_df = snowflake_session.create_dataframe(pandas_df)
        df = DataFrameConverter(snowflake_session).convert(snowpark_df, DataFrameType.PANDAS)
        assert_dataframes_equals(df, pandas_df)

    # ========================================
    # Unsupported Conversions (Snowpark <-> PySpark)
    # Note: These tests require PySpark to be installed
    # ========================================

    @pytest.mark.skipif(
        not PYSPARK_AVAILABLE,
        reason="Test requires PySpark to verify cross-backend conversion errors"
    )
    def test_convert_snowpark_to_pyspark_raises_error(self, spark, snowflake_session, pandas_df):
        """Test that converting Snowpark to PySpark raises UnsupportedConversionError"""
        snowpark_df = snowflake_session.create_dataframe(pandas_df)
        converter = DataFrameConverter(spark)
        with pytest.raises(UnsupportedConversionError) as exc_info:
            converter.convert(snowpark_df, DataFrameType.PYSPARK)
        assert "SNOWPARK to PYSPARK" in str(exc_info.value)

    @pytest.mark.skipif(
        not PYSPARK_AVAILABLE,
        reason="Test requires PySpark to verify cross-backend conversion errors"
    )
    def test_convert_snowpark_to_pandas_on_spark_raises_error(
        self, spark, snowflake_session, pandas_df
    ):
        """Test that converting Snowpark to Pandas-on-Spark raises UnsupportedConversionError"""
        snowpark_df = snowflake_session.create_dataframe(pandas_df)
        converter = DataFrameConverter(spark)
        with pytest.raises(UnsupportedConversionError) as exc_info:
            converter.convert(snowpark_df, DataFrameType.PANDAS_ON_SPARK)
        assert "SNOWPARK to PANDAS_ON_SPARK" in str(exc_info.value)

    # ========================================
    # Session Type Validation
    # ========================================

    def test_convert_pandas_to_snowpark_without_snowflake_session_raises_error(
        self, pandas_df
    ):
        """Test that converting Pandas to Snowpark without a SnowflakeSession raises ValueError"""
        converter = DataFrameConverter()  # No session provided
        with pytest.raises(ValueError) as exc_info:
            converter.convert(pandas_df, DataFrameType.SNOWPARK)
        assert "Snowflake session required" in str(exc_info.value)

