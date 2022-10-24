import pytest

from flypipe.data_type import String
from flypipe.datasource.spark import Spark
from flypipe.node import node
import pandas as pd
from pandas.testing import assert_frame_equal
from flypipe.converter.dataframe import DataFrameConverter
from flypipe.schema import Schema, Column


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark

    spark.createDataFrame(schema=('c0', 'c1'), data=[(0, 1,)]).createOrReplaceTempView('dummy_table1')

    return spark


class TestNode:

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
        assert node_input1.selected_columns == ('c1', 'c2')
        assert node_input2.selected_columns == ('c3',)
        assert a.output_columns == ['c1', 'c2', 'c3']

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

