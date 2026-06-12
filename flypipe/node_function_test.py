import pandas as pd
import pytest

from flypipe.node import node
from flypipe.node_function import NodeFunction, node_function
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

    def test_copy_memoizes_shared_node_function_dependency(self):
        """
        NodeFunction overrides Node.copy, so it must accept and thread the _memo
        parameter: InputNode.copy passes _memo positionally to self.node.copy
        whenever a node function sits in the dependency graph. A node function
        shared by two branches of a diamond must come out of the copy as a single
        shared object (not one duplicate per path), and its own node_dependencies
        must go through the same memo so ancestors reachable both through and
        around the node function stay shared too.
        """

        @node(type="pandas")
        def t1():
            return pd.DataFrame({"c1": [1]})

        @node_function(node_dependencies=[t1])
        def func():
            @node(type="pandas", dependencies=[t1])
            def internal(t1):
                return t1

            return internal

        @node(type="pandas", dependencies=[func])
        def left(func):
            return func

        @node(type="pandas", dependencies=[func])
        def right(func):
            return func

        @node(type="pandas", dependencies=[left, right, t1])
        def tail(left, right, t1):
            return left

        tail_copy = tail.copy()
        left_copy, right_copy, t1_input_copy = (
            input_node.node for input_node in tail_copy.input_nodes
        )

        func_copy = left_copy.input_nodes[0].node
        assert isinstance(func_copy, NodeFunction)
        assert func_copy is not func
        assert right_copy.input_nodes[0].node is func_copy

        # The node function's dependencies are threaded through the same memo, so
        # t1 reached through func and t1 reached directly are the same copy
        assert func_copy.node_dependencies[0] is not t1
        assert func_copy.node_dependencies[0] is t1_input_copy

    def test_copy_deep_chain_does_not_hit_recursion_limit(self):
        """
        NodeFunction shares Node.copy's explicit-stack traversal, so a chain of
        node functions far deeper than recursive descent allows (it overflowed
        Python's default recursion limit at a few hundred levels) must copy
        successfully and preserve the whole chain.
        """
        depth = 2000

        @node(type="pandas")
        def seed():
            return pd.DataFrame({"c1": [1]})

        chain = [seed]
        for i in range(1, depth):
            prev = chain[-1]

            def func():
                return None

            func.__name__ = f"f_{i}"
            chain.append(node_function(node_dependencies=[prev])(func))

        tail_copy = chain[-1].copy()

        length = 0
        current = tail_copy
        while isinstance(current, NodeFunction):
            length += 1
            assert current.key == chain[depth - length].key
            assert current is not chain[depth - length]
            current = current.node_dependencies[0]
        assert length == depth - 1  # every node-function link survived the copy
        assert current.key == seed.key
        assert current is not seed

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
