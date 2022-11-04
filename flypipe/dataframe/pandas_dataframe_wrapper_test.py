import numpy as np
import pandas as pd
import pytest
import datetime

from flypipe.exceptions import DataFrameMissingColumns
from flypipe.schema.types import Boolean, Decimal, Date, DateTime, Integer, Float, String
from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from pandas.testing import assert_frame_equal


class TestPandasDataFrameWrapper:

    def test_select_column_1(self):
        df = pd.DataFrame({
            'col1': [True, False],
            'col2': ['Hello', 'World'],
            'col3': ['Banana', 'Apple'],
        })
        expected_df = pd.DataFrame({
            'col1': [True, False],
            'col2': ['Hello', 'World'],
        })
        df_wrapper = DataFrameWrapper.get_instance(None, df)
        assert_frame_equal(df_wrapper.select_columns('col1', 'col2').df, expected_df)

    def test_select_column_2(self):
        df = pd.DataFrame({
            'col1': [True, False],
            'col2': ['Hello', 'World'],
            'col3': ['Banana', 'Apple'],
        })
        expected_df = pd.DataFrame({
            'col1': [True, False],
            'col2': ['Hello', 'World'],
        })
        df_wrapper = DataFrameWrapper.get_instance(None, df)
        assert_frame_equal(df_wrapper.select_columns(['col1', 'col2']).df, expected_df)

    def test_select_column_missing_column(self):
        df = pd.DataFrame({
            'col1': [True, False],
            'col2': ['Hello', 'World'],
            'col3': ['Banana', 'Apple'],
        })
        df_wrapper = DataFrameWrapper.get_instance(None, df)

        with pytest.raises(DataFrameMissingColumns):
            df_wrapper.select_columns(['col1', 'col4'])

    def test_get_column_flypipe_type(self, spark):
        df = pd.DataFrame({
            'c1': [True], 'c2': [1], 'c3': [1.1], 'c4': 'bla', 'c5': datetime.date(2022, 11, 4),
            'c6': datetime.datetime(2022, 11, 4, 12, 0, 0)})
        df_wrapper = DataFrameWrapper.get_instance(spark, df)
        assert isinstance(df_wrapper.get_column_flypipe_type('c1'), Boolean)
        assert isinstance(df_wrapper.get_column_flypipe_type('c2'), Integer)
        assert isinstance(df_wrapper.get_column_flypipe_type('c3'), Float)
        assert isinstance(df_wrapper.get_column_flypipe_type('c4'), String)
        assert isinstance(df_wrapper.get_column_flypipe_type('c5'), Date)
        assert isinstance(df_wrapper.get_column_flypipe_type('c6'), DateTime)

    def test_cast_column(self):
        """
        Ensure column casting works. We expect that all non-null values in the specified column to cast are converted
        to the destination type. Columns not specified in the cast should not be touched.
        """
        df = pd.DataFrame({
            'col1': [True, False, 1, 0, None, np.nan, np.NAN, pd.NA, pd.NaT],
            'col2': [1, 1, 0, 0, 0, 1, 1, 1, 1],
        })
        df_wrapper = DataFrameWrapper.get_instance(None, df)
        df_wrapper.cast_column('col1', Boolean())
        assert_frame_equal(df_wrapper.df, pd.DataFrame({
            'col1': [True, False, True, False, None, np.nan, np.NAN, pd.NA, pd.NaT],
            'col2': [1, 1, 0, 0, 0, 1, 1, 1, 1],
        }))
        assert pd.api.types.infer_dtype(df_wrapper.df['col1'], skipna=True) == 'boolean'

    def test_cast_column_invalid_value(self):
        """
        If any non-null values in a column to cast are invalid, in that they cannot be converted then we should throw
        an appropriate error.
        """
        df = pd.DataFrame({
            'col1': [True, 'rubbish', True, False],
        })
        df_wrapper = DataFrameWrapper.get_instance(None, df)
        with pytest.raises(ValueError) as ex:
            df_wrapper.cast_column('col1', Boolean())
        assert str(ex.value) == 'Invalid type Boolean for column col1, found incompatible row value "rubbish"'

    def test_cast_column_decimal(self):
        schema = None
        df_wrapper = DataFrameWrapper.get_instance(
            None, pd.DataFrame({'col1': ['A', 'B', 'C', 'D'], 'col2': [10, 56.66666666667, 5678, np.nan]}))
        df_wrapper.cast_column('col2', Decimal(3, 2))
        # TODO- looks like we're doing nothing with the precision?
        assert_frame_equal(df_wrapper.df, pd.DataFrame(
            {'col1': ['A', 'B', 'C', 'D'], 'col2': [10, 56.67, 5678, np.nan]}))
        assert df_wrapper.df.dtypes['col2'] == np.dtype('float64')

    def test_cast_column_datetime(self):
        schema = None
        df_wrapper = DataFrameWrapper.get_instance(None, pd.DataFrame({'col1': ['2022-10-31 20:30:35', np.nan]}))
        df_wrapper.cast_column('col1', DateTime())
        assert_frame_equal(df_wrapper.df, pd.DataFrame(
            {'col1': [pd.Timestamp(2022, 10, 31, 20, 30, 35), np.nan]},
            dtype=np.dtype('O'))
        )

    def test_cast_column_date(self):
        schema = None
        df_wrapper = DataFrameWrapper.get_instance(None, pd.DataFrame({'col1': ['2022-10-31 20:30:35', np.nan]}))
        df_wrapper.cast_column('col1', Date())
        assert_frame_equal(df_wrapper.df, pd.DataFrame(
            {'col1': [pd.Timestamp(2022, 10, 31, 20, 30, 35), np.nan]},
            dtype=np.dtype('O'))
        )

