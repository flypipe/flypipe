import numpy as np
import pandas as pd
from flypipe.data_type import Boolean
from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.schema import Schema, Column
from pandas.testing import assert_frame_equal


class TestSchema:

    def test_apply(self):
        schema = Schema([
            Column('column1', Boolean(), '')
        ])
        df = pd.DataFrame({'column1': [True, False, 1, 0, None, pd.NA, np.nan], 'column2': ['a', 'b', 'c']})

        expected = pd.DataFrame({'column1': [True, False, True, False, None, pd.NA, np.nan]})

        df_wrapper = DataFrameWrapper.get_instance(None, df)
        df_wrapper = schema.apply(df_wrapper)
        assert_frame_equal(df_wrapper.df, expected)