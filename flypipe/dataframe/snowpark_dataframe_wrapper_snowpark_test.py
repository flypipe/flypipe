"""Tests for SnowparkDataFrameWrapper - Snowpark functionality"""
import os
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from snowflake.snowpark.types import (
    StructType,
    ByteType,
    ShortType,
    StructField,
    BooleanType,
    IntegerType,
    BinaryType,
    LongType,
    FloatType,
    DoubleType,
    StringType,
    DecimalType,
    TimestampType,
    DateType,
)

from flypipe.dataframe.snowpark_dataframe_wrapper import SnowparkDataFrameWrapper
from flypipe.exceptions import DataFrameMissingColumns
from flypipe.schema.types import (
    Boolean,
    Decimal,
    Byte,
    Binary,
    Integer,
    Short,
    Long,
    Float,
    Double,
    String,
    DateTime,
    Date,
)


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") != "SNOWFLAKE",
    reason="Snowpark tests require RUN_MODE=SNOWFLAKE",
)
class TestSnowparkDataFrameWrapperSnowpark:
    """Tests for SnowparkDataFrameWrapper - Snowpark functionality"""

    def test_select_column_1(self, snowflake_session):
        """Test selecting columns using positional arguments"""
        df = snowflake_session.create_dataframe(
            pd.DataFrame({
                "col1": [True, False],
                "col2": ["Hello", "World"],
                "col3": ["Banana", "Apple"],
            })
        )
        expected_df = snowflake_session.create_dataframe(
            pd.DataFrame({
                "col1": [True, False],
                "col2": ["Hello", "World"],
            })
        )
        df_wrapper = SnowparkDataFrameWrapper.get_instance(None, df)
        result_df = df_wrapper.select_columns('"col1"', '"col2"').df
        
        # Compare as pandas DataFrames
        assert_frame_equal(
            result_df.to_pandas().sort_index(axis=1),
            expected_df.to_pandas().sort_index(axis=1),
            check_dtype=False
        )

    def test_select_column_2(self, snowflake_session):
        """Test selecting columns using a list"""
        df = snowflake_session.create_dataframe(
            pd.DataFrame({
                "col1": [True, False],
                "col2": ["Hello", "World"],
                "col3": ["Banana", "Apple"],
            })
        )
        expected_df = snowflake_session.create_dataframe(
            pd.DataFrame({
                "col1": [True, False],
                "col2": ["Hello", "World"],
            })
        )
        df_wrapper = SnowparkDataFrameWrapper.get_instance(None, df)
        result_df = df_wrapper.select_columns(['"col1"', '"col2"']).df
        
        # Compare as pandas DataFrames
        assert_frame_equal(
            result_df.to_pandas().sort_index(axis=1),
            expected_df.to_pandas().sort_index(axis=1),
            check_dtype=False
        )

    def test_select_column_missing_column(self, snowflake_session):
        """Test that selecting a missing column raises DataFrameMissingColumns"""
        df = snowflake_session.create_dataframe(
            pd.DataFrame({
                "col1": [True, False],
                "col2": ["Hello", "World"],
                "col3": ["Banana", "Apple"],
            })
        )
        df_wrapper = SnowparkDataFrameWrapper.get_instance(None, df)

        with pytest.raises(DataFrameMissingColumns):
            df_wrapper.select_columns(['"col1"', '"col4"'])

    def test_get_column_flypipe_type(self, snowflake_session):
        """Test that get_column_flypipe_type correctly identifies column types"""
        # Create a DataFrame with various types  
        # Note: Snowflake local testing mode converts some types (Int->Long, Float->Double)
        schema = StructType([
            StructField("c1", BooleanType()),
            StructField("c2", ByteType()),
            StructField("c3", BinaryType()),
            StructField("c4", IntegerType()),
            StructField("c5", ShortType()),
            StructField("c6", LongType()),
            StructField("c7", FloatType()),
            StructField("c8", DoubleType()),
            StructField("c9", StringType()),
            StructField("c10", DecimalType(13, 2)),
            StructField("c11", TimestampType()),
            StructField("c12", DateType()),
        ])
        
        df = snowflake_session.create_dataframe(
            [], schema=schema
        )
        df_wrapper = SnowparkDataFrameWrapper.get_instance(snowflake_session, df)

        print(df.schema)
        # Snowflake normalizes all integer types to NUMBER(38,0) (represented as LongType in Snowpark)
        # See: https://docs.snowflake.com/en/sql-reference/data-types-numeric
        # - ByteType (tinyint) → LongType (bigint)
        # - IntegerType (int) → LongType (bigint)
        # - ShortType (smallint) → LongType (bigint)
        # - LongType (bigint) → LongType (bigint) [unchanged]
        # Similarly, FloatType → DoubleType
        
        assert isinstance(df_wrapper.get_column_flypipe_type("C1"), Boolean)
        assert isinstance(df_wrapper.get_column_flypipe_type("C2"), Long)  # ByteType converted to Long
        assert isinstance(df_wrapper.get_column_flypipe_type("C3"), Binary)
        assert isinstance(df_wrapper.get_column_flypipe_type("C4"), Long)  # IntegerType converted to Long
        assert isinstance(df_wrapper.get_column_flypipe_type("C5"), Long)  # ShortType converted to Long
        assert isinstance(df_wrapper.get_column_flypipe_type("C6"), Long)
        assert isinstance(df_wrapper.get_column_flypipe_type("C7"), Double)  # FloatType converted to Double
        assert isinstance(df_wrapper.get_column_flypipe_type("C8"), Double)
        assert isinstance(df_wrapper.get_column_flypipe_type("C9"), String)
        c10_type = df_wrapper.get_column_flypipe_type("C10")
        assert isinstance(c10_type, Decimal)
        assert c10_type.precision == 13
        assert c10_type.scale == 2
        assert isinstance(df_wrapper.get_column_flypipe_type("C11"), DateTime)
        assert isinstance(df_wrapper.get_column_flypipe_type("C12"), Date)

    def test_cast_column(self, snowflake_session):
        """Test casting a column to a different type"""
        df = snowflake_session.create_dataframe(
            pd.DataFrame({"col1": [1, 0, None]})
        )
        df_wrapper = SnowparkDataFrameWrapper.get_instance(snowflake_session, df)
        df_wrapper.cast_column('"col1"', Boolean())
        
        expected_df = snowflake_session.create_dataframe(
            pd.DataFrame({"col1": [True, False, None]})
        )
        
        # Compare as pandas DataFrames
        assert_frame_equal(
            df_wrapper.df.to_pandas(),
            expected_df.to_pandas(),
            check_dtype=False
        )

    def test_cast_column_decimal(self, snowflake_session):
        """Test casting a column to Decimal type with specific precision and scale"""
        df = snowflake_session.create_dataframe(
            pd.DataFrame({
                "col1": [5678.12345, 999.11111, 1.2345678, None]
            })
        )
        df_wrapper = SnowparkDataFrameWrapper.get_instance(snowflake_session, df)
        df_wrapper.cast_column('"col1"', Decimal(5, 2))
        
        # Check the schema contains decimal type
        schema_dict = {field.name: field.datatype for field in df_wrapper.df.schema.fields}
        assert isinstance(schema_dict['"col1"'], DecimalType)
        assert schema_dict['"col1"'].precision == 5
        assert schema_dict['"col1"'].scale == 2

    def test_cast_column_date_with_snowflake_format(self, snowflake_session):
        """Test casting a string column to Date using Snowflake date format"""
        from flypipe.schema.util import DateFormat
        
        df = snowflake_session.create_dataframe(
            pd.DataFrame({
                "col1": ["2024-01-15", "2024-12-31", "2025-06-30"]
            })
        )
        df_wrapper = SnowparkDataFrameWrapper.get_instance(snowflake_session, df)
        
        # Use Snowflake format (YYYY-MM-DD)
        date_type = Date(format="YYYY-MM-DD", format_mode=DateFormat.SNOWFLAKE)
        df_wrapper.cast_column('"col1"', date_type)
        
        # Check the schema contains date type
        schema_dict = {field.name: field.datatype for field in df_wrapper.df.schema.fields}
        assert isinstance(schema_dict['"col1"'], DateType)

    def test_cast_column_datetime_with_snowflake_format(self, snowflake_session):
        """Test casting a string column to DateTime using Snowflake datetime format"""
        from flypipe.schema.util import DateFormat
        
        df = snowflake_session.create_dataframe(
            pd.DataFrame({
                "col1": ["2024-01-15 14:30:45", "2024-12-31 23:59:59", "2025-06-30 00:00:00"]
            })
        )
        df_wrapper = SnowparkDataFrameWrapper.get_instance(snowflake_session, df)
        
        # Use Snowflake format (YYYY-MM-DD HH24:MI:SS)
        datetime_type = DateTime(format="YYYY-MM-DD HH24:MI:SS", format_mode=DateFormat.SNOWFLAKE)
        df_wrapper.cast_column('"col1"', datetime_type)
        
        # Check the schema contains timestamp type
        schema_dict = {field.name: field.datatype for field in df_wrapper.df.schema.fields}
        assert isinstance(schema_dict['"col1"'], TimestampType)

