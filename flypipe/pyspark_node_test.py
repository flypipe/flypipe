import pandas
import pandas as pd
import pyspark.pandas
import pytest
from pyspark_test import assert_pyspark_df_equal

from tabulate import tabulate

from flypipe.data_type import Decimals, String
from flypipe.exceptions import SelectionNotFoundInDataFrame
from flypipe.datasource.spark import Spark
from flypipe.exceptions import NodeTypeInvalidError
from flypipe.node import node
from flypipe.schema.column import Column
from flypipe.schema.schema import Schema
from tests.utils.spark import drop_database


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
        with pytest.raises(NodeTypeInvalidError) as e_info:
            @node(type='anything', output=Schema([
                Column('balance', Decimals(16, 2), 'dummy')
            ]))
            def dummy():
                pass

    def test_end_to_end_adhoc(self, spark):

        @node(type='pyspark',
              dependencies=[Spark('dummy_table').select('c1')],
              output=Schema([
                  Column('c1', Decimals(16, 2), 'dummy'),
                  Column('c2', Decimals(16, 2), 'dummy')
              ]))
        def t1(dummy_table):
            raise Exception('I shouldnt be run!')

        @node(type='pyspark',
              dependencies=[t1.select('c1')],
              output=Schema([
                  Column('c1', Decimals(16, 2), 'dummy')
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
                  Column('c1', Decimals(16, 2), 'dummy'),
                  Column('c2', Decimals(16, 2), 'dummy'),
              ]))
        def t1(dummy_table):
            raise Exception('I shouldnt be run!')

        @node(type='pyspark',
              dependencies=[t1.select('c1')],
              output=Schema([
                  Column('c1', Decimals(16, 2), 'dummy')
              ]))
        def t2(t1):
            return t1.withColumn('c1', t1.c1 + 1)

        df = spark.createDataFrame(schema=('c1',), data=[(1,)])
        expected_df = spark.createDataFrame(schema=('c1',), data=[(2,)])

        assert_pyspark_df_equal(t2
                                .inputs({t1: df})
                                .run(spark, parallel=False),
                                expected_df)

    def test_end_to_end_full(self, spark):
        @node(type='pyspark',
              dependencies=[Spark('dummy_table').select('c1', 'c2')],
              output=Schema([
                  Column('c1', Decimals(16, 2), 'dummy'),
                  Column('c2', Decimals(16, 2), 'dummy')
              ]))
        def t1(dummy_table):
            return dummy_table

        @node(type='pyspark',
              dependencies=[t1.select('c1')],
              output=Schema([
                  Column('c1', Decimals(16, 2), 'dummy')
              ]))
        def t2(t1):
            return t1.withColumn('c1', t1.c1 + 1)

        expected_df = spark.createDataFrame(schema=('c1',), data=[(2,)])
        assert_pyspark_df_equal(t2.run(spark, parallel=False), expected_df)

    def test_skip_datasource(self, spark, mocker):
        """
        When we provide a dependency input to a node, not only does that node not need to be run but we also expect any
        dependencies of the provided node not to be run.

        When t1 has a dependency to a datasource and it is provided then datasource should not run

        """
        spark_dummy_table = Spark('dummy_table')
        @node(type='pyspark',
              dependencies=[spark_dummy_table.select('c1', 'c2')],
              output=Schema([
                  Column('c1', Decimals(16, 2), 'dummy'),
                  Column('c2', Decimals(16, 2), 'dummy')
              ]))
        def t1(dummy_table):
            return dummy_table

        func_name = spark_dummy_table.function.__name__
        spy = mocker.spy(spark_dummy_table, 'function')
        # Filthy hack to stop the spy removing the __name__ attribute from the function
        spark_dummy_table.function.__name__ = func_name

        df = spark.createDataFrame(schema=('c1', 'c2', 'c3'), data=[(1, 2, 3)])
        output_df = t1.inputs({Spark('dummy_table'): df}).run(spark, parallel=False)
        spy.assert_not_called()
        assert_pyspark_df_equal(output_df,
                                spark.createDataFrame(schema=('c1', 'c2',), data=[(1, 2,)]))

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
                  Column('c1', Decimals(16, 2), 'dummy'),
                  Column('c2', Decimals(16, 2), 'dummy')
              ]))
        def a(dummy_table):
            raise Exception('I shouldnt be run!')

        @node(type='pyspark', dependencies=[a.select('c1')], output=Schema([
            Column('c1', Decimals(16, 2), 'dummy')
        ]))
        def b(a):
            return a.withColumn('c1', a.c1 + 1)

        @node(type='pyspark',
              dependencies=[],
              output=Schema([
                  Column('c1', Decimals(16, 2), 'dummy')
              ]))
        def d():
            return spark.createDataFrame(schema=('c1',), data=[(6,), (7,)])

        @node(type='pyspark',
              dependencies=[b.select('c1'), d.select('c1')],
              output=Schema([Column('c1', Decimals(16, 2), 'dummy')]))
        def c(b, d):
            return b.union(d)

        df = spark.createDataFrame(schema=('c1',), data=[(4,), (5,)])
        expected_df = spark.createDataFrame(schema=('c1',), data=[(4,), (5,), (6,), (7,)])

        assert_pyspark_df_equal(c.inputs({b: df}).run(spark, parallel=False), expected_df)

    def test_datasource_basic(self, spark):
        stored_df = spark.createDataFrame(schema=('c1',), data=[(1,)])

        @node(
            type="pyspark",
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
        spark_dummy_table = Spark("dummy_table")
        @node(
            type="pyspark",
            dependencies=[
                spark_dummy_table.select('c1')
            ],
            output=Schema([
                Column('c1', Decimals(10, 2), 'dummy')
            ])
        )
        def t1(dummy_table):
            return dummy_table

        spark_dummy_table_2 = Spark("dummy_table")
        @node(
            type="pyspark",
            dependencies=[
                spark_dummy_table_2.select('c2')
            ],
            output=Schema([
                Column('c2', Decimals(10, 2), 'dummy')
            ])
        )
        def t2(dummy_table):
            return dummy_table

        @node(
            type="pyspark",
            dependencies=[t1.select('c1'), t2.select('c2')],
            output=Schema([
                Column('c1', Decimals(10, 2), 'dummy'),
                Column('c2', Decimals(10, 2), 'dummy')
            ])
        )
        def t3(t1, t2):
            return t1.join(t2)

        func_name = spark_dummy_table.function.__name__
        spy = mocker.spy(spark_dummy_table, 'function')
        # Filthy hack to stop the spy removing the __name__ attribute from the function
        spark_dummy_table.function.__name__ = func_name

        func_name2 = spark_dummy_table_2.function.__name__
        spy2 = mocker.spy(spark_dummy_table_2, 'function')
        # Filthy hack to stop the spy removing the __name__ attribute from the function
        spark_dummy_table_2.function.__name__ = func_name2

        t3.run(spark, parallel=False)
        spy.assert_called_once()
        spy2.assert_not_called()
        assert_pyspark_df_equal(spy.spy_return, spark.createDataFrame(schema=('c1', 'c2', 'c3'), data=[(1, 2, 3)]))

    def test_conversion_to_pandas(self, spark):

        @node(
            type="pyspark",
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

        @node(
            type="pandas",
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
        assert isinstance(df, pandas.DataFrame)

    def test_conversion_to_pandas_on_spark(self, spark):

        @node(
            type="pyspark",
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
            type="pandas_on_spark",
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
        assert isinstance(df, pyspark.pandas.DataFrame)

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

    def test_datasource_case_sensitive_columns(self, spark):
        """
        Test columns case sensitive

        Pandas dataframe
        import pandas as pd
        df = pd.DataFrame(data={"My_Col": [1]})
        print(df['My_Col']) #<-- passes
        print(df['my_col']) #<-- fails

        Pyspark Dataframe
        df = spark.createDataFrame(schema=('My_Col__x',), data=[(1,)])
        df.select('My_Col__x') #<-- passes
        df.select('my_col__x') #<-- fails


        """
        db_name = "test_pyspark_node"
        drop_database(spark, db_name)
        my_data = (
            spark
            .createDataFrame(schema=('My_Col__x', 'My_Col__y', 'My_Col__z',),
                             data=[("dummy","dummy","dummy",)])
        )

        spark.sql(f"create database if not exists {db_name}")
        (
            my_data
            .write.mode("overwrite")
            .saveAsTable(f"{db_name}.dummy_table__anything_c")
        )


        @node(type='pandas_on_spark',
              dependencies=[
                  Spark(f"{db_name}.dummy_table__anything_c")
                    .select('Id', 'my_col__x', 'My_Col__z')
              ],
              output=Schema([
                  Column('my_col', String(), 'dummy'),
              ]))
        def my_col(test_pyspark_node_dummy_table__anything_c):
            df = test_pyspark_node_dummy_table__anything_c
            df = df.rename(columns={'my_col__x': 'my_col'})
            return df

        with pytest.raises(SelectionNotFoundInDataFrame) as exc_info:
            (
                my_col
                    .clear_inputs()
                    .run(spark, parallel=False)
            )
        expected_error_df = pd.DataFrame(data = {
            'dataframe': [
               '',  'My_Col__x ', 'My_Col__z'
            ],
            'selection': [
                'Id', 'my_col__x', 'My_Col__z'
            ],
            'error': [
                'not found','Did you mean `My_Col__x`?','found'
            ]
        })
        expected_error_df = expected_error_df.sort_values([
            'selection',
            'dataframe'
        ]).reset_index(drop=True)
        assert str(exc_info.value) == \
               f"Flypipe: could not find some columns in the dataframe" \
                   f"\n\n{tabulate(expected_error_df, headers='keys', tablefmt='mixed_outline')}\n"

    def test_duplicated_output_columns(self, spark):

        @node(
            type="pandas_on_spark",
            dependencies=[Spark("dummy_table").select("c1", "c2")],
            output=Schema([
                Column("c1", String(), "dummy"),
                Column("c2", String(), "dummy")
        ]))
        def t1(dummy_table):
            return dummy_table

        @node(
            type="pandas_on_spark",
            dependencies=[t1.select("c1", "c2")],
            output=Schema([
                Column("c1", String(), "dummy"),
                Column("c2", String(), "dummy")
            ]))
        def t2(t1):
            return t1

        @node(
            type="pandas_on_spark",
            dependencies=[t1.select("c1")],
            output=Schema([
                Column("c1", String(), "dummy")
            ]))
        def t3(t1):
            return t1

        @node(
            type="pyspark",
            dependencies=[t2.select("c1", "c2"),
                          t1.select("c1")],
            output=Schema([
                Column("c2", String(), "dummy")
            ]))
        def t4(t1, t2):
            return t2


        t4._create_graph()
        for node_name in t4.node_graph.graph:
            node_ = t4.node_graph.get_node(node_name)
            if node_['transformation'].__name__ == "t4":
                continue
            assert len(node_['output_columns']) == len(set(node_['output_columns']))

        df = t4.run(spark, parallel=False)
        assert_pyspark_df_equal(df, spark.createDataFrame(schema=('c2',), data=[("2",)]))

    def test_dataframes_are_isolated_from_nodes(self, spark):
        import  pyspark.sql.functions as F

        @node(
            type="pyspark",
            output=Schema([
                Column('c1', String(), 'dummy'),
                Column('c2', String(), 'dummy')
            ])
        )
        def t1():
            return spark.createDataFrame(pandas.DataFrame(data={'c1': ["1"], 'c2': ["2"]}))

        @node(
            type="pyspark",
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
            t1 = t1.withColumn("c1", F.lit("t2 set this value"))
            t1 = t1.withColumn("c3", F.lit("t2 set this value"))
            return t1

        @node(
            type="pyspark",
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
            t1 = t1.toPandas()
            t2 = t2.toPandas()
            assert list(t1.columns) == ["c1", "c2"]
            assert t1.loc[0, "c1"] == "1"
            assert t1.loc[0, "c2"] == "2"
            assert list(t2.columns) == ["c1", "c2", "c3"]
            assert t2.loc[0, "c1"] == "t2 set this value"
            assert t2.loc[0, "c2"] == "2"
            assert t2.loc[0, "c3"] == "t2 set this value"
            return spark.createDataFrame(t2)

        df = t3.run(spark, parallel=False)