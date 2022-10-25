import pytest
import pandas as pd
from flypipe import node
from flypipe.utils import dataframe_type, DataFrameType


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark

    return spark


class TestPandasOnSparkNode:

    @pytest.mark.parametrize('extra_run_config,expected_df_type', [
        ({}, DataFrameType.PANDAS_ON_SPARK),
        ({'pandas_on_spark_use_pandas': False}, DataFrameType.PANDAS_ON_SPARK),
        ({'pandas_on_spark_use_pandas': True}, DataFrameType.PANDAS),
    ])
    def test_input_dataframes_type(self, spark, mocker, extra_run_config, expected_df_type):
        stub = mocker.stub()

        @node(
            type='pandas'
        )
        def t1():
            return pd.DataFrame({'fruit': ['Banana', 'Apple'], 'color': ['Yellow', 'Red']})

        @node(
            type='pyspark'
        )
        def t2():
            return spark.createDataFrame(schema=('name', 'fruit'), data=[('Chris', 'Banana')])

        @node(
            type='pandas_on_spark',
            dependencies=[
                t1.select('fruit', 'color'),
                t2.select('name', 'fruit'),
            ]
        )
        def t3(t1, t2):
            stub(t1, t2)
            return t1.merge(t2)

        t3.run(spark, parallel=False, **extra_run_config)
        assert dataframe_type(stub.call_args[0][0]) == expected_df_type
        assert dataframe_type(stub.call_args[0][1]) == expected_df_type
