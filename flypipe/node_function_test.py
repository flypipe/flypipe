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

    def test_expand_undefined_node_dependency(self):
        """
        If any dependencies in the node function are not returned in the node function and aren't defined in
        node_dependencies then throw an error.
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

    def test_expand(self):
        @node(type="pandas")
        def c():
            return pd.DataFrame({"c1": [1, 2], "c2": ["Joe", "John"]})

        @node(type="pandas", dependencies=[c])
        def b(c):
            return c

        @node(type="pandas", dependencies=[b, c.select("c2")])
        def a(b, c):
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

    def test_node_parameters2(self):
        @node(type="pandas")
        def a():
            return pd.DataFrame({"col1": [1]})

        @node(type="pandas")
        def b():
            return pd.DataFrame({"col1": [1]})

        @node_function(node_dependencies=[a])
        def t1():
            @node(type="pandas", dependencies=[b])
            def t1(b):
                return b

            return t1

        with pytest.raises(ValueError):
            t1.run()

        @node_function(node_dependencies=[a, b])
        def t2():
            @node(type="pandas", dependencies=[b])
            def t1(b):
                return b

            return t1

        with pytest.raises(ValueError):
            t2.run()

        @node_function(node_dependencies=[b])
        def t3():
            @node(type="pandas", dependencies=[a, b])
            def t1(a, b):
                return b

            return t1

        with pytest.raises(ValueError):
            t3.run()
