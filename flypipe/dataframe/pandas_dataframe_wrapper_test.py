# pylint: disable=duplicate-code
import datetime

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.exceptions import DataFrameMissingColumns
from flypipe.schema.types import (
    Boolean,
    Decimal,
    Date,
    DateTime,
    Integer,
    Float,
    String,
)


class TestPandasDataFrameWrapper:
    """Tests for Pandas Data Frame"""

    def test_select_column_1(self):
        df = pd.DataFrame(
            {
                "col1": [True, False],
                "col2": ["Hello", "World"],
                "col3": ["Banana", "Apple"],
            }
        )
        expected_df = pd.DataFrame(
            {
                "col1": [True, False],
                "col2": ["Hello", "World"],
            }
        )
        df_wrapper = DataFrameWrapper.get_instance(None, df)
        assert_frame_equal(df_wrapper.select_columns("col1", "col2").df, expected_df)

    def test_select_column_2(self):
        # pylint: disable=duplicate-code
        df = pd.DataFrame(
            {
                "col1": [True, False],
                "col2": ["Hello", "World"],
                "col3": ["Banana", "Apple"],
            }
        )
        expected_df = pd.DataFrame(
            {
                "col1": [True, False],
                "col2": ["Hello", "World"],
            }
        )
        # pylint: enable=duplicate-code
        df_wrapper = DataFrameWrapper.get_instance(None, df)
        assert_frame_equal(df_wrapper.select_columns(["col1", "col2"]).df, expected_df)

    def test_select_column_missing_column(self):
        df = pd.DataFrame(
            {
                "col1": [True, False],
                "col2": ["Hello", "World"],
                "col3": ["Banana", "Apple"],
            }
        )
        df_wrapper = DataFrameWrapper.get_instance(None, df)

        with pytest.raises(DataFrameMissingColumns):
            df_wrapper.select_columns(["col1", "col4"])

    def test_get_column_flypipe_type(self, spark):
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
        df_wrapper = DataFrameWrapper.get_instance(spark, df)
        assert isinstance(df_wrapper.get_column_flypipe_type("c1"), Boolean)
        assert isinstance(df_wrapper.get_column_flypipe_type("c2"), Integer)
        assert isinstance(df_wrapper.get_column_flypipe_type("c3"), Float)
        assert isinstance(df_wrapper.get_column_flypipe_type("c4"), String)
        assert isinstance(df_wrapper.get_column_flypipe_type("c5"), Date)
        assert isinstance(df_wrapper.get_column_flypipe_type("c6"), DateTime)

    def test_cast_column(self):
        """
        Ensure column casting works. We expect that all non-null values in the specified column to cast are converted
        to the destination type. Columns not specified in the cast should not be touched.
        """
        df = pd.DataFrame(
            {
                "col1": [True, False, 1, 0, None, np.nan, np.NAN, pd.NA, pd.NaT],
                "col2": [1, 1, 0, 0, 0, 1, 1, 1, 1],
            }
        )
        df_wrapper = DataFrameWrapper.get_instance(None, df)
        df_wrapper.cast_column("col1", Boolean())
        assert_frame_equal(
            df_wrapper.df,
            pd.DataFrame(
                {
                    "col1": [
                        True,
                        False,
                        True,
                        False,
                        None,
                        np.nan,
                        np.NAN,
                        pd.NA,
                        pd.NaT,
                    ],
                    "col2": [1, 1, 0, 0, 0, 1, 1, 1, 1],
                }
            ),
        )

        df = df_wrapper.df[df_wrapper.df["col1"].notnull()]
        assert pd.api.types.infer_dtype(df["col1"], skipna=True) == "boolean"

    def test_cast_column_invalid_value(self):
        """
        If any non-null values in a column to cast are invalid, in that they cannot be converted then we should throw
        an appropriate error.
        """
        df = pd.DataFrame(
            {
                "col1": [True, "rubbish", True, False],
            }
        )
        df_wrapper = DataFrameWrapper.get_instance(None, df)
        with pytest.raises(ValueError) as ex:
            df_wrapper.cast_column("col1", Boolean())
        assert (
            str(ex.value)
            == 'Invalid type Boolean for column col1, found incompatible row value "rubbish"'
        )

    def test_cast_column_integer(self):
        df_wrapper = DataFrameWrapper.get_instance(None, pd.DataFrame({"c1": [1.1]}))
        df_wrapper.cast_column("c1", Integer())
        assert df_wrapper.df.loc[0]["c1"] == 1
        assert df_wrapper.df.dtypes["c1"] == pd.Int64Dtype()

    def test_cast_column_integer_2(self):
        """
        Issue from https://pandas.pydata.org/docs/user_guide/integer_na.html#integer-na
        Pandas has a problem in that numpy.NaN is usually used to represent missing data, however it has a type of
        float. This means that an integer column with nan values resolves to type float. We need to use the extension
        pandas integer type to get around this.
        """
        df_wrapper = DataFrameWrapper.get_instance(
            None, pd.DataFrame({"c1": [1.1, np.nan, np.NAN, pd.NA, None]})
        )
        df_wrapper.cast_column("c1", Integer())
        assert_frame_equal(
            df_wrapper.df,
            pd.DataFrame({"c1": [1, pd.NA, pd.NA, pd.NA, pd.NA]}),
            check_dtype=False,
        )
        assert df_wrapper.df.dtypes["c1"] == pd.Int64Dtype()

    def test_cast_column_boolean(self):
        df_wrapper = DataFrameWrapper.get_instance(
            None, pd.DataFrame({"c1": [0, 1, np.nan]})
        )
        df_wrapper.cast_column("c1", Boolean())
        assert_frame_equal(
            df_wrapper.df,
            pd.DataFrame({"c1": [False, True, np.nan]}),
            check_dtype=False,
        )
        assert df_wrapper.df.dtypes["c1"] == np.dtype("O")

    def test_cast_column_decimal(self):
        df_wrapper = DataFrameWrapper.get_instance(
            None,
            pd.DataFrame(
                {
                    "col1": ["A", "B", "C", "D"],
                    "col2": [10, 56.66666666667, 5678, np.nan],
                }
            ),
        )
        df_wrapper.cast_column("col2", Decimal(3, 2))
        # TODO- looks like we're doing nothing with the precision?
        assert_frame_equal(
            df_wrapper.df,
            pd.DataFrame(
                {"col1": ["A", "B", "C", "D"], "col2": [10, 56.67, 5678, np.nan]}
            ),
        )
        assert df_wrapper.df.dtypes["col2"] == np.dtype("float64")

    def test_cast_column_datetime(self):
        df_wrapper = DataFrameWrapper.get_instance(
            None, pd.DataFrame({"col1": ["2022-10-31 20:30:35", np.nan]})
        )
        df_wrapper.cast_column("col1", DateTime())
        assert_frame_equal(
            df_wrapper.df,
            pd.DataFrame(
                {"col1": [pd.Timestamp(2022, 10, 31, 20, 30, 35), np.nan]},
                dtype=np.dtype("O"),
            ),
        )

    def test_cast_column_date(self):
        df_wrapper = DataFrameWrapper.get_instance(
            None, pd.DataFrame({"col1": ["2022-10-31 20:30:35", np.nan]})
        )
        df_wrapper.cast_column("col1", Date())
        assert_frame_equal(
            df_wrapper.df,
            pd.DataFrame(
                {"col1": [pd.Timestamp(2022, 10, 31, 20, 30, 35), np.nan]},
                dtype=np.dtype("O"),
            ),
        )


# pylint: enable=duplicate-code
