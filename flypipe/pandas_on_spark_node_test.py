import pandas
import pyspark.pandas
import pytest

from flypipe.data_type import Decimals, String
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
                Column('c1', Decimals(10, 2), 'dummy')
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
                Column('c1', Decimals(10, 2), 'dummy')
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
                Column('c1', Decimals(10, 2), 'dummy')
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
                Column('c1', Decimals(10, 2), 'dummy')
            ])
        )
        def t2(t1):
            return t1

        df = t2.run(spark, parallel=False)
        assert isinstance(df, pandas.DataFrame)

    def test_dataframes_are_isolated_from_nodes(self, spark):

        @node(
            type="pandas_on_spark",
            output=Schema([
                Column('c1', String(), 'dummy'),
                Column('c2', String(), 'dummy')
            ])
        )
        def t1():
            return spark.createDataFrame(pandas.DataFrame(data={'c1': ["1"], 'c2': ["2"]})).to_pandas_on_spark()

        @node(
            type="pandas_on_spark",
            dependencies=[
                t1.select("c1", "c2")
            ],
            output=Schema([
                Column('c1', String(), 'dummy'),
                Column('c2', String(), 'dummy'),
                Column('c3', String(), 'dummy'),
            ])
        )
        def t2(t1):
            t1["c1"] = "t2 set this value"
            t1["c3"] = t1["c1"]
            return t1

        @node(
            type="pandas_on_spark",
            dependencies=[
                t1.select("c1", "c2"),
                t2.select("c1", "c2", "c3")
            ],
            output=Schema([
                Column('c1', String(), 'dummy'),
                Column('c2', String(), 'dummy'),
                Column('c3', String(), 'dummy'),
            ])
        )
        def t3(t1, t2):
            assert list(t1.columns) == ["c1", "c2"]
            assert t1.loc[0, "c1"] == "1"
            assert t1.loc[0, "c2"] == "2"
            assert list(t2.columns) == ["c1", "c2", "c3"]
            assert t2.loc[0, "c1"] == "t2 set this value"
            assert t2.loc[0, "c2"] == "2"
            assert t2.loc[0, "c3"] == "t2 set this value"
            return t2

        df = t3.run(spark, parallel=False)

