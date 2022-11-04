import numpy as np
import pandas as pd
import pyspark.pandas as ps
import pytest
from pyspark.sql.types import StructField, StructType, BooleanType, ByteType, BinaryType, IntegerType, ShortType, \
    LongType, FloatType, DoubleType, StringType

from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.exceptions import DataFrameMissingColumns
from flypipe.schema.types import String, Boolean, Byte, Binary, Integer, Short, Long, Float, Double
from pandas.testing import assert_frame_equal


class TestPandasOnSparkDataFrameWrapper:

    def test_select_column_1(self):
        df = ps.DataFrame({
            'col1': [True, False],
            'col2': ['Hello', 'World'],
            'col3': ['Banana', 'Apple'],
        })
        expected_df = pd.DataFrame({
            'col1': [True, False],
            'col2': ['Hello', 'World'],
        })
        df_wrapper = DataFrameWrapper.get_instance(None, df)
        assert_frame_equal(df_wrapper.select_columns('col1', 'col2').df.to_pandas(), expected_df)

    def test_select_column_2(self):
        df = ps.DataFrame({
            'col1': [True, False],
            'col2': ['Hello', 'World'],
            'col3': ['Banana', 'Apple'],
        })
        expected_df = pd.DataFrame({
            'col1': [True, False],
            'col2': ['Hello', 'World'],
        })
        df_wrapper = DataFrameWrapper.get_instance(None, df)
        assert_frame_equal(df_wrapper.select_columns(['col1', 'col2']).df.to_pandas(), expected_df)

    def test_select_column_missing_column(self):
        df = ps.DataFrame({
            'col1': [True, False],
            'col2': ['Hello', 'World'],
            'col3': ['Banana', 'Apple'],
        })
        df_wrapper = DataFrameWrapper.get_instance(None, df)

        with pytest.raises(DataFrameMissingColumns):
            df_wrapper.select_columns(['col1', 'col4'])

    def test_get_column_flypipe_type(self, spark):
        df = spark.createDataFrame(schema=StructType([
            StructField('c1', BooleanType()),
            StructField('c2', ByteType()),
            StructField('c3', BinaryType()),
            StructField('c4', IntegerType()),
            StructField('c5', ShortType()),
            StructField('c6', LongType()),
            StructField('c7', FloatType()),
            StructField('c8', DoubleType()),
            StructField('c9', StringType()),
        ]), data=[]).to_pandas_on_spark()
        df_wrapper = DataFrameWrapper.get_instance(spark, df)

        assert isinstance(df_wrapper.get_column_flypipe_type('c1'), Boolean)
        assert isinstance(df_wrapper.get_column_flypipe_type('c2'), Byte)
        assert isinstance(df_wrapper.get_column_flypipe_type('c3'), Binary)
        assert isinstance(df_wrapper.get_column_flypipe_type('c4'), Integer)
        assert isinstance(df_wrapper.get_column_flypipe_type('c5'), Short)
        assert isinstance(df_wrapper.get_column_flypipe_type('c6'), Long)
        assert isinstance(df_wrapper.get_column_flypipe_type('c7'), Float)
        assert isinstance(df_wrapper.get_column_flypipe_type('c8'), Double)
        assert isinstance(df_wrapper.get_column_flypipe_type('c9'), String)

    def test_cast_column(self, spark):
        """
        Ensure column casting works. We expect that all non-null values in the specified column to cast are converted
        to the destination type. Columns not specified in the cast should not be touched.
        """
        pd_df = pd.DataFrame({
            'col1': [True, False, None, np.nan, np.NAN, pd.NA, pd.NaT],
            'col2': [1, 1, 0, 1, 1, 1, 1],
        })
        df = spark.createDataFrame(pd_df).to_pandas_on_spark()
        df_wrapper = DataFrameWrapper.get_instance(spark, df)
        df_wrapper.cast_column('col1', Boolean())
        assert_frame_equal(df_wrapper.df.to_pandas(), pd_df)
