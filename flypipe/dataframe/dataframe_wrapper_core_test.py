"""Tests for DataFrameWrapper - Core functionality"""

import os
import numpy as np
import pytest

from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.schema.types import Boolean, Decimal, String, Unknown


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


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") != "CORE",
    reason="Core tests require RUN_MODE=CORE",
)
class TestDataFrameWrapperCore:
    """Tests for DataFrameWrapper - Core functionality"""

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
