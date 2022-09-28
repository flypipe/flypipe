from flypipe.data_type import Decimal
from pyspark_test import assert_pyspark_df_equal


class TestSparkNode:

    def test_bla(self, spark):

        @node(type='spark', inputs=[
            Spark('fancy_table_1').select('account_id', 'balance')
            Spark('fancy_table_2').select('account_id', 'balance')
        ], output=Schema([
            Column('balance', Decimal(16, 2))
        ]))
        def balance(raw_fancy_table_1, raw_fancy_table_2):
            return raw_fancy_table_1.withColumn('balance', raw_fancy_table_1.balance + 1).select('balance')

        spark.createDataFrame(schema=('balance',), data=[(4,),(5,)]).createOrReplaceTempView('fancy_table_1')
        expected_df = spark.createDataFrame(schema=('balance',), data=[(5,),(6,)])

        df = balance.run(spark)

        assert_pyspark_df_equal(df, expected_df)

    def test_bla2(self, spark):
        @node(type='spark', output=Schema([
            Column('balance', Decimal(16, 2))
        ]))
        def balance(raw_fancy_table_1):
            return raw_fancy_table_1.withColumn('balance', raw_fancy_table_1.balance + 1).select('balance')

        spark.createDataFrame(schema=('balance',), data=[(4,), (5,)]).createOrReplaceTempView('fancy_table_1')
        expected_df = spark.createDataFrame(schema=('balance',), data=[(5,), (6,)])

        df = balance.run(spark, raw_fancy_table_1=bla, raw_fancy_table_2=fancy_table_2, use_cache=False)

        assert_pyspark_df_equal(df, expected_df)
