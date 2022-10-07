import pandas
import pyspark.pandas
import pytest
from pytest_mock import mocker

from flypipe.data_type import Decimal
from pyspark_test import assert_pyspark_df_equal

from flypipe.datasource.spark import Spark
from flypipe.exceptions import ErrorDependencyNoSelectedColumns, ErrorNodeTypeInvalid
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


class TestPySparkNode:

    def test_exception_invalid_node_type(self, spark):
        with pytest.raises(ErrorNodeTypeInvalid) as e_info:
            @node(type='anything', output=Schema([
                Column('balance', Decimal(16, 2))
            ]))
            def dummy():
                pass

    def test_exception_not_specified_dependencies(self, spark):
        @node(type='pyspark', output=Schema([
            Column('balance', Decimal(16, 2))
        ]))
        def dummy():
            pass


        with pytest.raises(ErrorDependencyNoSelectedColumns) as e_info:
            @node(type='pyspark', dependencies=[dummy], output=Schema([
                Column('balance', Decimal(16, 2))
            ]))
            def balance(dummy):
                return dummy.withColumn('balance', dummy.balance + 1).select('balance')

    def test_end_to_end_adhoc(self, spark):

        @node(type='pyspark',
              dependencies=[Spark('dummy_table').select('c1')],
              output=Schema([
                  Column('c1', Decimal(16, 2)),
                  Column('c2', Decimal(16, 2))
              ]))
        def t1(dummy_table):
            raise Exception('I shouldnt be run!')

        @node(type='pyspark',
              dependencies=[t1.select('c1')],
              output=Schema([
                  Column('c1', Decimal(16, 2))
              ]))
        def t2(t1):
            return t1.withColumn('c1', t1.c1 + 1)

        df = spark.createDataFrame(schema=('c1',), data=[(1,)])
        expected_df = spark.createDataFrame(schema=('c1',), data=[(2,)])

        assert_pyspark_df_equal(t2(df), expected_df)

    def test_end_to_end_partial(self, spark):
        @node(type='pyspark',
              dependencies=[Spark('dummy_table').select('c1')],
              output=Schema([
                  Column('c1', Decimal(16, 2)),
                  Column('c2', Decimal(16, 2))
              ]))
        def t1(dummy_table):
            raise Exception('I shouldnt be run!')

        @node(type='pyspark',
              dependencies=[t1.select('c1')],
              output=Schema([
                  Column('c1', Decimal(16, 2))
              ]))
        def t2(t1):
            return t1.withColumn('c1', t1.c1 + 1)

        df = spark.createDataFrame(schema=('c1',), data=[(1,)])
        expected_df = spark.createDataFrame(schema=('c1',), data=[(2,)])

        assert_pyspark_df_equal(t2.inputs(t1=df).run(spark, parallel=False), expected_df)

    def test_end_to_end_full(self, spark):
        @node(type='pyspark',
              dependencies=[Spark('dummy_table').select('c1')],
              output=Schema([
                  Column('c1', Decimal(16, 2)),
                  Column('c2', Decimal(16, 2))
              ]))
        def t1(dummy_table):
            return dummy_table

        @node(type='pyspark',
              dependencies=[t1.select('c1')],
              output=Schema([
                  Column('c1', Decimal(16, 2))
              ]))
        def t2(t1):
            return t1.withColumn('c1', t1.c1 + 1)

        expected_df = spark.createDataFrame(schema=('c1',), data=[(2,)])

        assert_pyspark_df_equal(t2.run(spark, parallel=False), expected_df)

    def test_skip_upstream(self, spark):
        """
        When we provide a dependency input to a node, not only does that node not need to be run but we also expect any
        dependencies of the provided node not to be run.

        a - b
              \
               c
              /
            d
       When b is provided and we process c, only c and d should be run.
        """

        @node(type='pyspark',
              dependencies=[Spark('dummy_table').select('c1')],
              output=Schema([
                  Column('c1', Decimal(16, 2)),
                  Column('c2', Decimal(16, 2))
              ]))
        def a(dummy_table):
            raise Exception('I shouldnt be run!')

        @node(type='pyspark', dependencies=[a.select('c1')], output=Schema([
            Column('c1', Decimal(16, 2))
        ]))
        def b(a):
            return a.withColumn('c1', a.c1 + 1)

        @node(type='pyspark',
              dependencies=[Spark('dummy_table').select('c1')],
              output=Schema([
                  Column('c1', Decimal(16, 2)),
                  Column('c2', Decimal(16, 2))
              ]))
        def d(dummy_table):
            return spark.createDataFrame(schema=('c1',), data=[(6,), (7,)])

        @node(type='pyspark',
              dependencies=[b.select('c1'), d.select('c1')],
              output=Schema([Column('c1', Decimal(16, 2))]))
        def c(b, d):
            return b.union(d)

        df = spark.createDataFrame(schema=('c1',), data=[(4,), (5,)])
        expected_df = spark.createDataFrame(schema=('c1',), data=[(4,), (5,), (6,), (7,)])

        assert_pyspark_df_equal(c.inputs(b=df).run(spark, parallel=False), expected_df)

    def test_datasource_basic(self, spark):
        stored_df = spark.createDataFrame(schema=('c1',), data=[(1,)])

        @node(
            type="pyspark",
            dependencies=[
                Spark("dummy_table").select('c1')
            ],
            output=Schema([
                Column('c1', Decimal(10, 2))
            ])
        )
        def t1(dummy_table):
            return dummy_table

        df = t1.run(spark, parallel=False)
        assert_pyspark_df_equal(df, stored_df)

    def test_datasource_consolidate_columns(self, spark, mocker):
        """
        When multiple nodes use a single table in a datasource then we expect that:
        a) the requested columns consolidate into a single query (i.e one query for col1 and col2 and not a query for
        each column)
        b) the query ONLY includes columns that nodes have requested.

        Therefore in the below scenario where we have a node requesting table.c1 and another node requested table.c2, we
        expect one query to table for both columns and for the other column c3 in the table not to be included.
        """

        @node(
            type="pyspark",
            dependencies=[
                Spark.table("dummy_table").select('c1')
            ],
            output=Schema([
                Column('c1', Decimal(10, 2))
            ])
        )
        def t1(dummy_table):
            return dummy_table

        @node(
            type="pyspark",
            dependencies=[
                Spark.table("dummy_table").select('c2')
            ],
            output=Schema([
                Column('c2', Decimal(10, 2))
            ])
        )
        def t2(dummy_table):
            return dummy_table

        @node(
            type="pyspark",
            dependencies=[t1.select('c1'), t2.select('c2')],
            output=Schema([
                Column('c1', Decimal(10, 2)),
                Column('c2', Decimal(10, 2))
            ])
        )
        def t3(t1, t2):
            return t1.join(t2)

        func_name = Spark.get_instance('dummy_table').func.function.__name__
        spy = mocker.spy(Spark.get_instance('dummy_table').func, 'function')
        # Filthy hack to stop the spy removing the __name__ attribute from the function
        Spark.get_instance('dummy_table').func.function.__name__ = func_name

        t3.run(spark, parallel=False)
        spy.assert_called_once()
        assert_pyspark_df_equal(spy.spy_return, spark.createDataFrame(schema=('c1', 'c2'), data=[(1, 2)]))

    def test_conversion_to_pandas(self, spark):

        @node(
            type="pyspark",
            dependencies=[
                Spark("dummy_table").select('c1')
            ],
            output=Schema([
                Column('c1', Decimal(10, 2))
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
                Column('c1', Decimal(10, 2))
            ])
        )
        def t2(t1):
            return t1

        df = t2.run(spark, parallel=False)
        assert isinstance(df, pandas.DataFrame)

        @node(
            type="pandas",
            dependencies=[
                Spark("dummy_table").select('c1')
            ],
            output=Schema([
                Column('c1', Decimal(10, 2))
            ])
        )
        def t1(dummy_table):
            return dummy_table

        df = t1.run(spark, parallel=False)
        assert isinstance(df, pandas.DataFrame)

    def test_conversion_to_pandas_on_spark(self, spark):

        @node(
            type="pyspark",
            dependencies=[
                Spark("dummy_table").select('c1')
            ],
            output=Schema([
                Column('c1', Decimal(10, 2))
            ])
        )
        def t1(dummy_table):
            return dummy_table

        @node(
            type="pandas_on_spark",
            dependencies=[
                t1.select('c1')
            ],
            output=Schema([
                Column('c1', Decimal(10, 2))
            ])
        )
        def t2(t1):
            return t1

        df = t2.run(spark, parallel=False)
        assert isinstance(df, pyspark.pandas.DataFrame)

        @node(
            type="pandas_on_spark",
            dependencies=[
                Spark("dummy_table").select('c1')
            ],
            output=Schema([
                Column('c1', Decimal(10, 2))
            ])
        )
        def t1(dummy_table):
            return dummy_table

        df = t1.run(spark, parallel=False)
        assert isinstance(df, pyspark.pandas.DataFrame)

