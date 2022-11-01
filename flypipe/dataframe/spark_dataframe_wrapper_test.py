import pandas as pd
import numpy as np
import pytest
from pandas.testing import assert_frame_equal
from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from pyspark_test import assert_pyspark_df_equal
from flypipe.schema.types import Boolean, Decimal


@pytest.fixture
def spark():
    from tests.utils.spark import spark
    return spark


class TestSparkDataFrameWrapper:

    def test_cast_column(self, spark):
        df = spark.createDataFrame(schema=('col1',), data=[
            (1,),
            (0,),
            (None,)
        ])
        df_wrapper = DataFrameWrapper.get_instance(spark, df)
        df_wrapper.cast_column('col1', Boolean())
        assert_pyspark_df_equal(df_wrapper.df, spark.createDataFrame(schema=('col1',), data=[
            (True,),
            (False,),
            (None,)
        ]))
        assert df_wrapper.df.dtypes == [('col1', 'boolean')]

    def test_cast_column_decimal(self, spark):
        df = spark.createDataFrame(schema=('col1',), data=[
            (5678.12345,),
            (999.11111,),
            (1.2345678,),
            (None,)
        ])
        df_wrapper = DataFrameWrapper.get_instance(spark, df)
        df_wrapper.cast_column('col1', Decimal(5, 2))
        assert df_wrapper.df.dtypes[0] == ('col1', 'decimal(5,2)')
        # TODO: should probably not resort to a pandas conversion + df check but I can't seem to create a pyspark df with DecimalType and the below literals.
        assert_frame_equal(df_wrapper.df.toPandas(), pd.DataFrame({'col1': [None, 999.11, 1.23, None]}, dtype=np.dtype('O')))
