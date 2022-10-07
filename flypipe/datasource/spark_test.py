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
            .createDataFrame(schema=('c1', 'c2', 'c3'), data=[(1, 2, 3)])
            .createOrReplaceTempView('dummy_table')
    )
    return spark


class TestSparkDataSource:
    def test_load(self, spark):
        schema = Schema([
                  Column('c1', Decimals(16, 2))
                 ])

        @node(type='pyspark',
              dependencies=[Spark('dummy_table').select('c1')],
              output=schema)
        def t1(dummy_table):
            return dummy_table

        df_expected = spark.createDataFrame(schema=('c1',), data=[(1,)])
        assert_pyspark_df_equal(df_expected, t1.run(spark, parallel=False))