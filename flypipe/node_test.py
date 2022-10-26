import pytest
from flypipe.datasource.spark import Spark
import pandas as pd
from flypipe.node import node, Node
from pandas.testing import assert_frame_equal
from flypipe.converter.dataframe import DataFrameConverter
from flypipe.pandas_on_spark_node import PandasOnSparkNode
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
        ('pandas_on_spark', PandasOnSparkNode),
    ])
    def test_get_class(self, node_type, expected_class):
        assert Node.get_class(node_type) == expected_class

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

