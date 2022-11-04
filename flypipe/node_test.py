import pytest
import pandas as pd
import pyspark.pandas as ps
from pyspark_test import assert_pyspark_df_equal
from pandas.testing import assert_frame_equal
from tabulate import tabulate
from flypipe.data_type import String
from flypipe.datasource.spark import Spark
from flypipe.converter.dataframe import DataFrameConverter
from flypipe.exceptions import DataFrameMissingColumns
from flypipe.node import node, Node
from pandas.testing import assert_frame_equal
from flypipe.schema import Schema, Column
from flypipe.schema.types import String, Decimal, Integer
from flypipe.utils import DataFrameType, dataframe_type


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark

    spark.createDataFrame(schema=('c0', 'c1'), data=[(0, 1,)]).createOrReplaceTempView('dummy_table1')

    return spark


class TestNode:

    @pytest.mark.parametrize('node_type,expected_class', [
        ('pyspark', Node),
        ('pandas', Node),
        ('pandas_on_spark', Node),
    ])
    def test_get_class(self, node_type, expected_class):
        assert type(Node(None, node_type)) == expected_class

    def test_select(self):
        """
        Ensure that when we call select on a node we get a wrapped object containing the node and the selected columns.
        Make sure the selected columns are local to the instance being queried and don't leak into sibling nodes.
        """
        @node(
            type='pandas'
        )
        def a():
            return

        node_input1 = a.select('c1', 'c2')
        node_input2 = a.select('c3')
        assert node_input1.__name__ == 'a'
        assert node_input2.__name__ == 'a'
        assert node_input1.selected_columns == ['c1', 'c2']
        assert node_input2.selected_columns == ['c3']

    def test_select_column(self, spark):
        """
        Ensure that:
        a) when we select a subset of columns from a parent transformation we only receive those columns in the
        transformation to be run.
        b) when we have multiple nodes selecting different columns from the same parent node the columns don't leak
        into other transformations.
        """
        data = pd.DataFrame({'fruit': ['apple', 'banana'], 'color': ['red', 'yellow']})
        @node(
            type='pandas',
        )
        def a():
            return data

        @node(
            type='pandas',
            dependencies=[a.select('fruit')]
        )
        def b(a):
            return a

        @node(
            type='pandas',
            dependencies=[a.select('color')]
        )
        def c(a):
            return a

        assert_frame_equal(b.run(spark, parallel=False), data[['fruit']])
        assert_frame_equal(c.run(spark, parallel=False), data[['color']])

    def test_conversion_after_output_column_filter(self, spark, mocker):
        """
        a) When processing the output of a node we only select columns which are requested by child nodes.
        b) When processing a child node we convert all incoming input dataframes from parent nodes to the same type as
        the child node.

        We want to ensure that we do b) the filtering done in a) is already done. This is vital because sometimes the
        original output of a node can be far bigger than the output with filtered columns, if the order of operations is wrong
        and b) happens before a) it can be extremely inefficient.
        """
        @node(
            type='pandas',
            dependencies=[Spark('dummy_table1').select('c1')]
        )
        def t1(dummy_table1):
            return dummy_table1

        spy = mocker.spy(DataFrameConverter, 'convert')
        t1.run(spark, parallel=False)
        assert spy.call_args.args[1].columns == ['c1']

    def test_alias(self):
        """
        Ensure we can set up a node dependency with an alias.
        """
        @node(
            type='pandas'
        )
        def t1():
            return pd.DataFrame({'fruit': ['banana', 'apple'], 'color': ['yellow', 'red']})

        @node(
            type='pandas',
            dependencies=[t1.select('fruit').alias('my_fruits')]
        )
        def t2(my_fruits):
            return my_fruits

        # No assertions are required, if the alias doesn't work then t2 will crash when run as the argument signature
        # won't align with what it's expecting.
        t2.run(parallel=False)

    @pytest.mark.parametrize('extra_run_config,expected_df_type', [
        ({}, DataFrameType.PANDAS_ON_SPARK),
        ({'pandas_on_spark_use_pandas': False}, DataFrameType.PANDAS_ON_SPARK),
        ({'pandas_on_spark_use_pandas': True}, DataFrameType.PANDAS),
    ])
    def test_input_dataframes_type(self, spark, mocker, extra_run_config, expected_df_type):
        stub = mocker.stub()

        @node(
            type='pandas'
        )
        def t1():
            return pd.DataFrame({'fruit': ['Banana', 'Apple'], 'color': ['Yellow', 'Red']})

        @node(
            type='pyspark'
        )
        def t2():
            return spark.createDataFrame(schema=('name', 'fruit'), data=[('Chris', 'Banana')])

        @node(
            type='pandas_on_spark',
            dependencies=[
                t1.select('fruit', 'color'),
                t2.select('name', 'fruit'),
            ]
        )
        def t3(t1, t2):
            stub(t1, t2)
            return t1.merge(t2)

        t3.run(spark, parallel=False, **extra_run_config)
        assert dataframe_type(stub.call_args[0][0])==expected_df_type
        assert dataframe_type(stub.call_args[0][1])==expected_df_type

    def test_key(self):
        """
        Ensure that different nodes with the same function name have different keys
        """
        class A:
            @classmethod
            @node(
                type='pandas',
                output=Schema([
                        Column('fruit', String(), '')
                    ])
            )
            def test(cls):
                return pd.DataFrame({'fruit': ['banana']})

        class B:
            class C:
                @classmethod
                @node(
                    type='pandas',
                    dependencies=[A.test.select('fruit')],
                    output=Schema([
                        Column('fruit', String(), '')
                    ])
                )
                def test(cls, test):
                    return test['fruit']

        assert A.test.key == 'flypipe_node_test_function_test_TestNode_test_key__locals__A_test'
        assert B.C.test.key == 'flypipe_node_test_function_test_TestNode_test_key__locals__B_C_test'


    def test_duplicated_selected(self):
        """
        Ensure throw exception if selected duplicated columns
        """

        @node(
            type='pandas'
        )
        def t1():
            return pd.DataFrame({'fruit': ['banana', 'apple'], 'color': ['yellow', 'red']})

        with pytest.raises(ValueError):
            @node(
                type='pandas',
                dependencies=[t1.select('fruit', 'fruit').alias('my_fruits')]
            )
            def t2(my_fruits):
                return my_fruits


    def test_alias_run_with_keys_and_alias_in_function(self):
        """
        Ensure that node graph is processed with node keys and alias is used for arguments
        """
        from tests.transformations.group_1.t1 import t1
        from tests.transformations.group_2.t1 import t1 as t1_group2

        @node(
            type="pandas",
            dependencies=[
                t1.select("c1"),
                t1_group2.select("c1").alias("t1_group2")
            ],
            output = Schema([
                Column("c1_group1_t1", String(), 'dummy'),
                Column("c1_group2_t1", String(), 'dummy'),
            ])
        )
        def t3(t1, t1_group2):
            t1['c1_group1_t1'] = t1['c1']
            t1['c1_group2_t1'] = t1_group2['c1']

            return t1

        df = t3.run(parallel=False)
        assert df.loc[0, 'c1_group1_t1'] == "t0 group_1_t1"
        assert df.loc[0, 'c1_group2_t1'] == "t0 group_2_t1"

        t1_df = pd.DataFrame(data={'c1': ['t0 group_1_t1']})
        t1_group2_df = pd.DataFrame(data={'c1': ['t0 group_2_t1']})

        df = (
            t3
                .inputs({
                t1: t1_df,
                t1_group2: t1_group2_df
            })
                .run(parallel=False)
        )

        assert df.loc[0, 'c1_group1_t1']=="t0 group_1_t1"
        assert df.loc[0, 'c1_group2_t1']=="t0 group_2_t1"


    def test_run_dataframe_conversion(self, spark):
        """
        If a node is dependant upon a node of a different dataframe type, then we expect the output of the parent node
        to be converted when it's provided to the child node.
        """
        @node(
            type="pandas_on_spark",
            output=Schema([
                Column('c1', Decimal(10, 2))
            ])
        )
        def t1():
            return spark.createDataFrame(pd.DataFrame(data={'c1': [1], 'c2': [2], 'c3': [3]})).to_pandas_on_spark()

        @node(
            type="pandas",
            dependencies=[t1.select('c1')]
        )
        def t2(t1):
            return t1

        t1_output = t1.run(spark, parallel=False)
        t2_output = t2.run(spark, parallel=False)
        assert isinstance(t1_output, ps.frame.DataFrame)
        assert isinstance(t2_output, pd.DataFrame)

    def test_run_input_dataframes_isolation(self):
        """
        Suppose we have a node with an output x. We provide this output as input to a second node and do some tweaks to
        it. We want to ensure that the basic output is not affected by the tweaks done by the second node.
        """

        @node(
            type="pandas",
            output=Schema([
                Column('c1', String()),
                Column('c2', String())
            ])
        )
        def t1():
            return pd.DataFrame(data={'c1': ["1"], 'c2': ["2"]})

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
                Column('c1', String()),
                Column('c2', String()),
                Column('c3', String()),
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

        t3.run(spark, parallel=False)

    def test_adhoc_call(self, spark):
        """
        If we call a node directly with a function call we should skip calling the input dependencies and instead use
        the passed in arguments
        """

        @node(type='pyspark',
              dependencies=[Spark('dummy_table').select('c1')],
              output=Schema([
                  Column('c1', Decimal(16, 2), 'dummy'),
                  Column('c2', Decimal(16, 2), 'dummy')
              ]))
        def t1(dummy_table):
            raise Exception('I shouldnt be run!')

        @node(type='pyspark',
              dependencies=[t1.select('c1')],
              output=Schema([
                  Column('c1', Decimal(16, 2), 'dummy')
              ]))
        def t2(t1):
            return t1.withColumn('c1', t1.c1 + 1)

        df = spark.createDataFrame(schema=('c1',), data=[(1,)])
        expected_df = spark.createDataFrame(schema=('c1',), data=[(2,)])

        assert_pyspark_df_equal(t2(df), expected_df)

    def test_run_skip_input(self):
        """
        If we manually provide a dataframe via the input function prior to running a node then dependent transformation
        that ordinarily generates the input dataframe should be skipped.
        """
        @node(type='pandas',
              output=Schema([
                  Column('c1', Integer(), 'dummy'),
                  Column('c2', Integer(), 'dummy'),
              ]))
        def t1():
            raise Exception('I shouldnt be run!')

        @node(type='pandas',
              dependencies=[t1.select('c1')],
              output=Schema([
                  Column('c1', Integer(), 'dummy')
              ]))
        def t2(t1):
            t1['c1'] = t1['c1'] + 1
            return t1

        df = pd.DataFrame({'c1': [1]})
        expected_df = pd.DataFrame({'c1': [2]})

        assert_frame_equal(
            t2.run(inputs={t1: df}, parallel=False),
            expected_df
        )

    def test_run_skip_input_2(self, spark):
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
                  Column('c1', Integer(), 'dummy'),
                  Column('c2', Integer(), 'dummy')
              ]))
        def a(dummy_table):
            raise Exception('I shouldnt be run!')

        @node(type='pyspark', dependencies=[a.select('c1')], output=Schema([
            Column('c1', Integer(), 'dummy')
        ]))
        def b(a):
            return a.withColumn('c1', a.c1 + 1)

        @node(type='pyspark',
              dependencies=[],
              output=Schema([
                  Column('c1', Integer(), 'dummy')
              ]))
        def d():
            return spark.createDataFrame(schema=('c1',), data=[(6,), (7,)])

        @node(type='pyspark',
              dependencies=[b.select('c1'), d.select('c1')],
              output=Schema([Column('c1', Integer(), 'dummy')]))
        def c(b, d):
            return b.union(d)

        df = spark.createDataFrame(schema=('c1',), data=[(4,), (5,)])
        expected_df = spark.createDataFrame(schema=('c1',), data=[(4,), (5,), (6,), (7,)])

        assert_pyspark_df_equal(c.run(spark, inputs={b: df}, parallel=False), expected_df, check_dtype=False)

    def test_run_missing_column(self):
        """
        If the schema requests a column which the output dataframe does not provide we expect it to error.
        """
        @node(
            type='pandas',
            output=Schema([
                Column('c1', String()),
                Column('c2', String()),
            ])
        )
        def t1():
            return pd.DataFrame({'c1': ['Hello', 'World']})
        with pytest.raises(DataFrameMissingColumns):
            t1.run(parallel=False)

