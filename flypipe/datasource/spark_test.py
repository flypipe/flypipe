import pytest
from pyspark_test import assert_pyspark_df_equal

from flypipe import node
from flypipe.data_type import Decimals
from flypipe.datasource.spark import Spark
from flypipe.schema import Schema, Column


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark

    (
        spark
            .createDataFrame(schema=('c0','c1'), data=[(0, 1,)])
            .createOrReplaceTempView('dummy_table1')
    )

    (
        spark
            .createDataFrame(schema=('c2',), data=[(2,)])
            .createOrReplaceTempView('dummy_table2')
    )

    (
        spark
            .createDataFrame(schema=('c3',), data=[(3,)])
            .createOrReplaceTempView('dummy_table3')
    )
    return spark


class TestSparkDataSource:
    def test_load(self, spark):
        schema = Schema([
                  Column('c1', Decimals(16, 2))
                 ])

        @node(type='pyspark',
              dependencies=[Spark('dummy_table1').select('c1')],
              output=schema)
        def t1(dummy_table1):
            return dummy_table1

        df_expected = spark.createDataFrame(schema=('c1',), data=[(1,)])
        assert_pyspark_df_equal(df_expected, t1.run(spark, parallel=False))

    def test_multiple_sources(self, spark):
        @node(type='pyspark',
              dependencies=[Spark('dummy_table1').select('c0')],
              output=Schema([
                  Column('c0', Decimals(16, 2))])
              )
        def t0(dummy_table1):
            return dummy_table1

        @node(type='pyspark',
              dependencies=[Spark('dummy_table1').select('c1')],
              output=Schema([
                  Column('c1', Decimals(16, 2))])
              )
        def t1(dummy_table1):
            return dummy_table1

        @node(type='pyspark',
              dependencies=[Spark('dummy_table2').select('c2')],
              output=Schema([
                  Column('c2', Decimals(16, 2))])
              )
        def t2(dummy_table2):
            return dummy_table2

        @node(type='pyspark',
              dependencies=[Spark('dummy_table3').select('c3')],
              output=Schema([
                  Column('c3', Decimals(16, 2))])
              )
        def t3(dummy_table3):
            return dummy_table3

        @node(type='pyspark',
              dependencies=[
                  t0.select('c0'),
                  t1.select('c1'),
                  t2.select('c2'),
                  t3.select('c3'),
              ],
              output=Schema([
                  Column('c0', Decimals(16, 2)),
                  Column('c1', Decimals(16, 2)),
                  Column('c2', Decimals(16, 2)),
                  Column('c3', Decimals(16, 2)),
                  ])
              )
        def t4(t0, t1, t2, t3):
            return (
                t0
                .join(t1)
                .join(t2)
                .join(t3)
            )

        df_expected = spark.createDataFrame(schema=('c0', 'c1', 'c2', 'c3',), data=[(0, 1, 2, 3)])
        assert_pyspark_df_equal(df_expected, t4.run(spark, parallel=False))


