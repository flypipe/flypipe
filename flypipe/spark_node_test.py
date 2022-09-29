import pytest
from flypipe.data_type import Decimal
from pyspark_test import assert_pyspark_df_equal

from flypipe.node import node
from flypipe.schema.column import Column
from flypipe.schema.schema import Schema


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark

    return spark


class TestSparkNode:
    #
    # def test_bla(self, spark):
    #
    #     @node(type='spark', inputs=[
    #         Spark('fancy_table_1').select('account_id', 'balance')
    #         Spark('fancy_table_2').select('account_id', 'balance')
    #     ], output=Schema([
    #         Column('balance', Decimal(16, 2))
    #     ]))
    #     def balance(raw_fancy_table_1, raw_fancy_table_2):
    #         return raw_fancy_table_1.withColumn('balance', raw_fancy_table_1.balance + 1).select('balance')
    #
    #     spark.createDataFrame(schema=('balance',), data=[(4,),(5,)]).createOrReplaceTempView('fancy_table_1')
    #     expected_df = spark.createDataFrame(schema=('balance',), data=[(5,),(6,)])
    #
    #     df = balance.run(spark)
    #
    #     assert_pyspark_df_equal(df, expected_df)

    def test_end_to_end_adhoc(self, spark):
        @node(type='pyspark', output=Schema([
            Column('balance', Decimal(16, 2))
        ]))
        def dummy():
            raise Exception('I shouldnt be run!')

        @node(type='pyspark', dependencies=[dummy], output=Schema([
            Column('balance', Decimal(16, 2))
        ]))
        def balance(dummy):
            return dummy.withColumn('balance', dummy.balance + 1).select('balance')

        df = spark.createDataFrame(schema=('balance',), data=[(4,), (5,)])
        expected_df = spark.createDataFrame(schema=('balance',), data=[(5,), (6,)])

        assert_pyspark_df_equal(balance(df), expected_df)

    def test_end_to_end_partial(self, spark):
        @node(type='pyspark', output=Schema([
            Column('balance', Decimal(16, 2))
        ]))
        def dummy():
            raise Exception('I shouldnt be run!')

        @node(type='pyspark', dependencies=[dummy], output=Schema([
            Column('balance', Decimal(16, 2))
        ]))
        def balance(dummy):
            return dummy.withColumn('balance', dummy.balance + 1).select('balance')

        df = spark.createDataFrame(schema=('balance',), data=[(4,), (5,)])
        expected_df = spark.createDataFrame(schema=('balance',), data=[(5,), (6,)])

        assert_pyspark_df_equal(balance.inputs(dummy=df).run(spark, parallel=False), expected_df)

    def test_end_to_end_full(self, spark):

        @node(type='pyspark', output=Schema([
            Column('balance', Decimal(16, 2))
        ]))
        def balance():
            output = spark.createDataFrame(schema=('balance',), data=[(4,), (5,)])
            return output.withColumn('balance', output.balance + 1).select('balance')

        @node(type='pyspark', dependencies=[balance], output=Schema([
            Column('balance', Decimal(16, 2))
        ]))
        def add(balance):
            return balance.withColumn('balance', balance.balance + 1)

        expected_df = spark.createDataFrame(schema=('balance',), data=[(6,), (7,)])

        assert_pyspark_df_equal(add.run(spark, parallel=False), expected_df)

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

        @node(type='pyspark', output=Schema([
            Column('dummy', Decimal(16, 2))
        ]))
        def a(raw_fancy_table_1):
            raise Exception('I shouldnt be run!')

        @node(type='pyspark', dependencies=[a], output=Schema([
            Column('dummy', Decimal(16, 2))
        ]))
        def b(a):
            return a.withColumn('dummy', a.dummy + 1)

        @node(type='pyspark', output=Schema([
            Column('dummy', Decimal(16, 2))
        ]))
        def d():
            return spark.createDataFrame(schema=('dummy',), data=[(6,), (7,)])

        @node(type='pyspark', dependencies=[b, d], output=Schema([
            Column('dummy', Decimal(16, 2))
        ]))
        def c(b, d):
            return b.union(d)

        df = spark.createDataFrame(schema=('dummy',), data=[(4,), (5,)])
        expected_df = spark.createDataFrame(schema=('dummy',), data=[(4,), (5,), (6,), (7,)])

        assert_pyspark_df_equal(c.inputs(b=df).run(spark, parallel=False), expected_df)
