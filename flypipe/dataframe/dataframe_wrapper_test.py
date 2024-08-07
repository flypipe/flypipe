import numpy as np
import pandas as pd
import pytest

from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.dataframe.pandas_dataframe_wrapper import PandasDataFrameWrapper
from flypipe.dataframe.pandas_on_spark_dataframe_wrapper import (
    PandasOnSparkDataFrameWrapper,
)
from flypipe.dataframe.spark_dataframe_wrapper import SparkDataFrameWrapper
from flypipe.schema.types import Boolean, Decimal, String, Unknown
from flypipe.tests.spark import spark


class DummyDataFrameWrapper(DataFrameWrapper):
    """Dummy subclass of abstract class DataFrameWrapper so we can use it in tests"""

    FLYPIPE_TYPE_TO_DF_TYPE_MAP = {Boolean.key(): np.dtype("bool")}

    def _select_columns(self, columns):
        pass

    def copy(self):
        pass

    def get_column_flypipe_type(self, target_column):
        return Unknown()

    def _cast_column(self, column, flypipe_type, df_type):
        pass

    def _cast_column_decimal(self, column, flypipe_type):
        pass


class TestDataFrameWrapper:
    """Tests for DataFrameWrapper"""

    @pytest.mark.parametrize(
        "df,expected_class",
        [
            (pd.DataFrame({"column": [1]}), PandasDataFrameWrapper),
            (
                spark.createDataFrame(schema=["column"], data=[[1]]),
                SparkDataFrameWrapper,
            ),
            (
                spark.createDataFrame(schema=["column"], data=[[1]]).pandas_api(),
                PandasOnSparkDataFrameWrapper,
            ),
        ],
    )
    def test_get_instance(self, df, expected_class):
        assert isinstance(DataFrameWrapper.get_instance(spark, df), expected_class)

    @pytest.mark.parametrize(
        "df",
        [
            pd.DataFrame({"col1": [1], "col2": [2]}),
            spark.createDataFrame(schema=("col1", "col2"), data=[[1, 2]]),
            spark.createDataFrame(schema=("col1", "col2"), data=[[1, 2]]).pandas_api(),
        ],
    )
    def test_select_columns_out_of_place(self, df):
        """
        Ensure that DataFrameWrapper.select_columns does the selection operation out-of-place and returns a new
        dataframe wrapper, therefore the original dataframe wrapper should be untouched.
        """
        # TODO- doesn't look like we're testing anything here?
        df_wrapper = DataFrameWrapper.get_instance(spark, df)
        df_wrapper2 = df_wrapper.select_columns("col1")  # noqa: F841

    def test_cast_column_basic(self, mocker):
        """
        If a cast is requested that has a direct type mapping then we expect the general _cast_column function to be
        invoked.
        """
        wrapper = DummyDataFrameWrapper(None, None)
        spy = mocker.spy(DummyDataFrameWrapper, "_cast_column")
        flypipe_type = Boolean()
        wrapper.cast_column("c1", flypipe_type)
        assert spy.call_count == 1
        assert spy.call_args[0] == (wrapper, "c1", flypipe_type, np.dtype("bool"))

    def test_cast_column_custom(self, mocker):
        """
        If a cast is requested that is not in the simple type map but we have a custom method for it the algorithm
        should use it.
        """
        wrapper = DummyDataFrameWrapper(None, None)
        spy = mocker.spy(DummyDataFrameWrapper, "_cast_column_decimal")
        wrapper.cast_column("c1", Decimal())
        assert spy.call_count == 1

    def test_cast_column_missing_cast(self):
        """
        If a cast is requested that is not in the simple type map and for which we don't have a custom cast method, we
        expect an appropriate error.
        """
        wrapper = DummyDataFrameWrapper(None, None)
        with pytest.raises(TypeError) as ex:
            wrapper.cast_column("c1", String())
        assert (
            str(ex.value)
            == "Unable to cast to flypipe type String- no dataframe type registered"
        )
