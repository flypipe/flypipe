import pandas as pd
import pytest

from flypipe.node import node
from flypipe.node_function import node_function
from flypipe.run_context import RunContext
from flypipe.schema import Schema, Column
from flypipe.schema.types import String


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

    def test_node_function_output(self):
        col1 = Column("col1", String(), "test")

        @node_function(output=Schema(col1))
        def t1(param1=1, param2=2):
            @node(type="pandas")
            def t2():
                assert param1 == 10 and param2 == 20
                return pd.DataFrame()

            return t2

        assert t1.output_schema.col1 == col1

    def test_node_function_output_is_not_same_as_node_output_if_declared_raise_valuerror(
        self,
    ):

        col1 = Column("col1", String(), "test")
        col2 = Column("col2", String(), "test")

        @node(type="pandas", output=Schema(col2))
        def t2():
            pass

        @node_function(output=Schema(col1))
        def t1():
            return t2

        run_context = RunContext()

        with pytest.raises(ValueError):
            t1.create_graph(run_context)

    def test_node_function_output_is_the_same_then_passes(self):
        col1 = Column("col1", String(), "test")

        @node(type="pandas", output=Schema(col1))
        def t2():
            pass

        @node_function(output=Schema(col1))
        def t1():
            return t2

        run_context = RunContext()
        t1.create_graph(run_context)
        execution_graph = t1.node_graph.get_execution_graph(run_context)
        end_node_name = execution_graph.get_end_node_name(execution_graph.graph)
        end_node = execution_graph.get_node(end_node_name)["transformation"]
        assert t1.output_schema == end_node.output_schema

    def test_node_function_output_is_set_but_return_node_has_no_output_use_node_function_output_to_returned_node(
        self,
    ):
        col1 = Column("col1", String(), "test")

        @node_function(output=Schema(col1))
        def t1():
            @node(type="pandas")
            def t2():
                pass

            return t2

        run_context = RunContext()
        t1.create_graph(run_context)
        execution_graph = t1.node_graph.get_execution_graph(run_context)
        end_node_name = execution_graph.get_end_node_name(execution_graph.graph)

        end_node = execution_graph.get_node(end_node_name)["transformation"]
        assert t1.output_schema == end_node.output_schema

    def test_node_function_output_is_none_but_return_node_has_output(self):
        col1 = Column("col1", String(), "test")

        @node_function()
        def t1():
            @node(type="pandas", output=Schema(col1))
            def t2():
                pass

            return t2

        run_context = RunContext()
        t1.create_graph(run_context)
        execution_graph = t1.node_graph.get_execution_graph(run_context)
        end_node_name = execution_graph.get_end_node_name(execution_graph.graph)

        end_node = execution_graph.get_node(end_node_name)["transformation"]
        assert t1.output_schema is None
        assert end_node.output_schema is not None
