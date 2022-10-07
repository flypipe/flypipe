import pandas
import pyspark.pandas
import pytest

from flypipe.data_type import Decimals
from flypipe.datasource.spark import Spark
from flypipe.node import node
from flypipe.schema.column import Column
from flypipe.schema.schema import Schema


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark

    (
        spark
            .createDataFrame(schema=('c1', 'c2', 'c3'), data=[(1, 2, 3)])
            .createOrReplaceTempView('dummy_table')
    )
    return spark


class TestPandasOnSparkNode:

    def test_conversion_to_pyspark(self, spark):

        @node(
            type="pandas_on_spark",
            dependencies=[
                Spark("dummy_table").select('c1')
            ],
            output=Schema([
                Column('c1', Decimals(10, 2))
            ])
        )
        def t1(dummy_table):
            return dummy_table

        df = t1.run(spark, parallel=False)
        assert isinstance(df, pyspark.pandas.DataFrame)

        @node(
            type="pyspark",
            dependencies=[
                t1.select('c1')
            ],
            output=Schema([
                Column('c1', Decimals(10, 2))
            ])
        )
        def t2(t1):
            return t1

        df = t2.run(spark, parallel=False)
        assert isinstance(df, pyspark.sql.DataFrame)


    def test_conversion_to_pandas(self, spark):

        @node(
            type="pandas_on_spark",
            dependencies=[
                Spark("dummy_table").select('c1')
            ],
            output=Schema([
                Column('c1', Decimals(10, 2))
            ])
        )
        def t1(dummy_table):
            return dummy_table

        @node(
            type="pandas",
            dependencies=[
                t1.select('c1')
            ],
            output=Schema([
                Column('c1', Decimals(10, 2))
            ])
        )
        def t2(t1):
            return t1

        df = t2.run(spark, parallel=False)
        assert isinstance(df, pandas.DataFrame)


