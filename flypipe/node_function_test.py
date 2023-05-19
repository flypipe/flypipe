import pandas as pd
import pytest

from flypipe.node import node
from flypipe.node_function import node_function


class TestNodeFunction:
    """Tests for NodeFunction"""

    def test_node_dependencies_type(self):
        """
        For simplicity we want to limit the node dependencies on node functions to only have dependent nodes and not
        the column selection which is what we normally do on regular nodes. Let's throw an appropriate error if the
        user attempts to make the node dependent with columns
        """

        @node(type="pandas")
        def test():
            return pd.DataFrame({"c1": [1, 2], "c2": ["Joe", "John"]})

        with pytest.raises(TypeError):

            @node_function(node_dependencies=[test.select("c1")])
            def func():
                pass

    def test_expand(self):
        @node(type="pandas")
        def c():
            return pd.DataFrame({"c1": [1, 2], "c2": ["Joe", "John"]})

        @node(type="pandas", dependencies=[c])
        def b(c):
            return c

        @node(type="pandas", dependencies=[b, c.select("c2")])
        def a(b, c):  # pylint: disable=unused-argument
            return b

        @node_function(node_dependencies=[c])
        def func():
            return a, b

        nodes = func.expand(None)

        assert nodes == [a, b]

    def test_node_parameters(self):
        @node_function()
        def t1(param1=1, param2=2):
            @node(type="pandas")
            def t2():
                assert param1 == 10 and param2 == 20
                return pd.DataFrame()

            return t2

        t1.run(parameters={t1: {"param1": 10, "param2": 20}})

    def test_expand_mismatched_node_dependency_1(self):
        """
        Any node dependencies on external nodes (i.e nodes defined outside the node function) must be defined in the
        node_function decorator, in the node_dependencies parameter. An error should be issued if an external node is a
        dependency and isn't in the node_function decorator.
        """

        @node(type="pandas")
        def b():
            return pd.DataFrame({"c1": [1, 2], "c2": ["Joe", "John"]})

        @node_function()
        def func():
            @node(type="pandas", dependencies=[b])
            def a(b):
                return b

            return a

        with pytest.raises(ValueError):
            func.expand(None)

    def test_expand_mismatched_node_dependency_2(self):
        """
        Inverse of test_expand_mismatched_node_dependency_1, if a node is defined in the node_function decorator as an
        external dependency, Flypipe expects that at least one of the nodes the node function returns will have this
        node as a node dependency, an exception will be thrown if this isn't the case.
        """
        @node(type="pandas")
        def t0():
            return pd.DataFrame(data={"col1": [1]})

        @node_function(node_dependencies=[t0])
        def node_f():
            @node(
                type="pandas",
            )
            def t1():
                return pd.DataFrame(data={"col1": [2]})

            return t1

        with pytest.raises(ValueError):
            node_f.run()
