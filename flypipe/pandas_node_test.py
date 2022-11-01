import pandas
import pyspark.pandas
import pytest
from tabulate import tabulate

from flypipe.data_type import Decimals, String
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

    def test_conversion_from_pandas_to_pandas_on_spark(self, spark, pandas_df):

        @node(
            type="pandas_on_spark",
            output=Schema([
                Column('c1', Decimals(10, 2), 'dummy')
            ])
        )
        def t1():
            return pandas.DataFrame(data={'c1': [1], 'c2': [2], 'c3': [3]})

        df = t1.run(spark, parallel=False)
        assert isinstance(df, pyspark.pandas.frame.DataFrame)


    def test_conversion_to_pyspark(self, spark, pandas_df):

        @node(
            type="pyspark",
            output=Schema([
                Column('c1', Decimals(10, 2), 'dummy')
            ])
        )
        def t1():
            return pandas_df

        df = t1.run(spark, parallel=False)
        assert isinstance(df, pyspark.sql.DataFrame)

    def test_dataframes_are_isolated_from_nodes(self):

        @node(
            type="pandas",
            output=Schema([
                Column('c1', String(), 'dummy'),
                Column('c2', String(), 'dummy')
            ])
        )
        def t1():
            return pandas.DataFrame(data={'c1': ["1"], 'c2': ["2"]})

        @node(
            type="pandas",
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
            type="pandas",
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




