import pytest

from flypipe.node import node
import pandas as pd
from pandas.testing import assert_frame_equal


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark

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

    def test_consolidated_query(self):
        pass
