"""Tests for DataFrameConverter - PySpark functionality"""
import os
import pandas as pd
import pytest

from flypipe import node
from flypipe.converter.dataframe import DataFrameConverter, UnsupportedConversionError
from flypipe.schema import Column, Schema
from flypipe.schema.types import Boolean
from flypipe.utils import assert_dataframes_equals, DataFrameType


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") not in ["SPARK", "SPARK_CONNECT"],
    reason="PySpark tests require RUN_MODE=SPARK or SPARK_CONNECT",
)
class TestDataFrameConverterPySpark:
    """Tests on DataFrameConverter - PySpark functionality"""

    @pytest.fixture(scope="function")
    def pandas_df(self):
        return pd.DataFrame(data={"col1": [1, 2, 3], "col2": ["1a", "2a", "3a"]})

    @pytest.fixture(scope="function")
    def pyspark_df(self, spark, pandas_df):
        return spark.createDataFrame(pandas_df)

    @pytest.fixture(scope="function")
    def pandas_on_spark_df(self, pyspark_df):
        return pyspark_df.pandas_api()

    # ========================================
    # Pandas to PySpark Conversions
    # ========================================

    def test_convert_pandas_to_pandas_on_spark(
        self, spark, pandas_df, pandas_on_spark_df
    ):
        """Test converting Pandas DataFrame to Pandas-on-Spark DataFrame"""
        df = DataFrameConverter(spark).convert(pandas_df, DataFrameType.PANDAS_ON_SPARK)
        assert_dataframes_equals(df, pandas_on_spark_df)

    def test_convert_pandas_to_pyspark(self, spark, pandas_df, pyspark_df):
        """Test converting Pandas DataFrame to PySpark DataFrame"""
        df = DataFrameConverter(spark).convert(pandas_df, DataFrameType.PYSPARK)
        assert_dataframes_equals(df, pyspark_df)

    # ========================================
    # Pandas-on-Spark Conversions
    # ========================================

    def test_convert_pandas_on_spark_to_pandas(
        self, spark, pandas_on_spark_df, pandas_df
    ):
        """Test converting Pandas-on-Spark DataFrame to Pandas DataFrame"""
        df = DataFrameConverter(spark).convert(pandas_on_spark_df, DataFrameType.PANDAS)
        assert_dataframes_equals(df, pandas_df)

    def test_convert_pandas_on_spark_to_pyspark(
        self, spark, pandas_on_spark_df, pyspark_df
    ):
        """Test converting Pandas-on-Spark DataFrame to PySpark DataFrame"""
        df = DataFrameConverter(spark).convert(
            pandas_on_spark_df, DataFrameType.PYSPARK
        )
        assert_dataframes_equals(df, pyspark_df)

    # ========================================
    # PySpark Conversions
    # ========================================

    def test_convert_pyspark_to_pandas(self, spark, pyspark_df, pandas_df):
        """Test converting PySpark DataFrame to Pandas DataFrame"""
        df = DataFrameConverter(spark).convert(pyspark_df, DataFrameType.PANDAS)
        assert_dataframes_equals(df, pandas_df)

    def test_convert_pyspark_to_pandas_on_spark(
        self, spark, pyspark_df, pandas_on_spark_df
    ):
        """Test converting PySpark DataFrame to Pandas-on-Spark DataFrame"""
        df = DataFrameConverter(spark).convert(
            pyspark_df, DataFrameType.PANDAS_ON_SPARK
        )
        assert_dataframes_equals(df, pandas_on_spark_df)

    # ========================================
    # Unsupported Conversions (PySpark -> Snowpark)
    # ========================================

    def test_convert_pyspark_to_snowpark_raises_error(self, spark, pyspark_df, snowflake_session):
        """Test that converting PySpark to Snowpark raises UnsupportedConversionError"""
        converter = DataFrameConverter(snowflake_session)
        with pytest.raises(UnsupportedConversionError) as exc_info:
            converter.convert(pyspark_df, DataFrameType.SNOWPARK)
        assert "PYSPARK to SNOWPARK" in str(exc_info.value)

    def test_convert_pandas_on_spark_to_snowpark_raises_error(
        self, spark, pandas_on_spark_df, snowflake_session
    ):
        """Test that converting Pandas-on-Spark to Snowpark raises UnsupportedConversionError"""
        converter = DataFrameConverter(snowflake_session)
        with pytest.raises(UnsupportedConversionError) as exc_info:
            converter.convert(pandas_on_spark_df, DataFrameType.SNOWPARK)
        assert "PANDAS_ON_SPARK to SNOWPARK" in str(exc_info.value)

    # ========================================
    # Session Type Validation
    # ========================================

    def test_convert_pandas_to_pyspark_without_spark_session_raises_error(self, pandas_df):
        """Test that converting Pandas to PySpark without a SparkSession raises ValueError"""
        converter = DataFrameConverter()  # No session provided
        with pytest.raises(ValueError) as exc_info:
            converter.convert(pandas_df, DataFrameType.PYSPARK)
        assert "PySpark SparkSession required" in str(exc_info.value)

    # ========================================
    # Edge Cases
    # ========================================

    @pytest.mark.parametrize(
        "node_type",
        ["pyspark", "pandas_on_spark"],
    )
    def test_empty_dataframe_to_spark(self, spark, node_type):
        """
        Test that empty DataFrames can be converted from Pandas to PySpark/Pandas-on-Spark.
        
        This test verifies that the conversion handles empty DataFrames without crashing
        and logs an appropriate warning.
        """
        from unittest.mock import patch
        
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

        # Mock the logger to capture the warning
        with patch("flypipe.converter.dataframe.logger.warning") as mock_warning:
            result = t2.run(spark)
            
            # Verify we got a result back
            assert result is not None
            
            # Verify the warning was logged
            expected_warning = (
                "pyspark.errors.exceptions.base.PySparkValueError: [CANNOT_INFER_EMPTY_SCHEMA] Can not infer "
                "schema from empty Pandas Dataframe to Pyspark Dataframe => Creating empty dataset with "
                "StringType() for all columns"
            )
            mock_warning.assert_called_once_with(expected_warning)

