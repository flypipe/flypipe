import pandas
import pyspark.pandas
import pytest

from flypipe.data_type import Decimals
from flypipe.node import node
from flypipe.schema.column import Column
from flypipe.schema.schema import Schema


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark
    return spark

@pytest.fixture(scope="function")
def pandas_df():
    return pandas.DataFrame(data={'c1': [1], 'c2': [2], 'c3': [3]})

class TestPandasOnSparkNode:

    def test_conversion_to_pandas_on_spark(self, spark, pandas_df):
        @node(
            type="pandas_on_spark",
            output=Schema([
                Column('c1', Decimals(10, 2))
            ])
        )
        def t1():
            return pandas_df

        df = t1.inputs(dummy_table=pandas_df).run(spark, parallel=False)
        assert isinstance(df, pyspark.pandas.DataFrame)


    def test_conversion_to_pyspark(self, spark, pandas_df):

        @node(
            type="pyspark",
            output=Schema([
                Column('c1', Decimals(10, 2))
            ])
        )
        def t1():
            return pandas_df

        df = t1.inputs(dummy_table=pandas_df).run(spark, parallel=False)
        assert isinstance(df, pyspark.sql.DataFrame)
