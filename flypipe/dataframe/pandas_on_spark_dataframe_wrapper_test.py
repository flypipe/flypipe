import numpy as np
import pandas as pd
from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.schema.types import String, Boolean
from pandas.testing import assert_frame_equal


class TestPandasOnSparkDataFrameWrapper:

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
