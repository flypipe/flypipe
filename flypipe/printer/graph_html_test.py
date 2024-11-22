import pandas as pd

from flypipe import node
from flypipe.printer.graph_html import GraphHTML
from flypipe.run_context import RunContext
from flypipe.schema import Schema, Column
from flypipe.schema.types import String, Integer


class TestNodeGraph:
    """Tests for NodeGraph"""

    def test_get_node_positions_1(self):
        @node(type="pandas")
        def t1():
            return

        @node(type="pandas", dependencies=[t1.select("dummy")])
        def t2():
            return

        @node(type="pandas", dependencies=[t2.select("dummy")])
        def t3():
            return

        t3.create_graph(run_context=RunContext())
        positions = GraphHTML(t3.node_graph).get_node_positions()
        assert positions == {
            t1.key: [1.0, 50.0],
            t2.key: [2.0, 47.5],
            t3.key: [3.0, 50.0],
        }

    def test_get_node_positions_2(self):
        @node(type="pandas")
        def t1():
            return

        @node(type="pandas", dependencies=[t1.select("dummy")])
        def t2():
            return

        @node(type="pandas", dependencies=[t1.select("dummy")])
        def t3():
            return

        @node(type="pandas", dependencies=[t2.select("dummy"), t3.select("dummy")])
        def t4():
            return

        # TODO- we should not be having to call a private method to setup
        t4.create_graph(run_context=RunContext())
        positions = GraphHTML(t4.node_graph).get_node_positions()
        assert positions == {
            t1.key: [1.0, 50.0],
            t2.key: [2.0, 30.83],
            t3.key: [2.0, 64.16],
            t4.key: [3.0, 50.0],
        }

    def test_get_node_columns_schema_defined(self):
        """
        If the schema is defined on a node then we will extract the column list for a node from it.
        """

        @node(
            type="pandas",
            output=Schema(
                [
                    Column("c1", String()),
                    Column("c2", Integer()),
                ]
            ),
        )
        def t1():
            return pd.DataFrame({"c1": ["Bla"], "c2": [1]})

        t1.create_graph(run_context=RunContext())
        assert GraphHTML(t1.node_graph)._get_node_columns(t1.key) == [
            {"name": "c1", "type": "String", "description": ""},
            {"name": "c2", "type": "Integer", "description": ""},
        ]

    def test_get_node_columns_from_dependencies(self):
        """
        If the schema for a node is not provided then we will look at which columns successor nodes are requesting and
        build the column list from them. Note this assumes that all of the columns successors are requesting exist on
        the node.
        """

        @node(
            type="pandas",
        )
        def t1():
            return pd.DataFrame({"c1": ["Bla"], "c2": [1], "c3": [1]})

        @node(type="pandas", dependencies=[t1.select("c1")])
        def t2(t1):
            return t1

        @node(type="pandas", dependencies=[t1.select("c3")])
        def t3(t1):
            return t1

        @node(type="pandas", dependencies=[t2, t3])
        def t4(t2, t3):
            return t2

        t4.create_graph(run_context=RunContext())
        assert GraphHTML(t4.node_graph)._get_node_columns(t1.key) == [
            {"name": "c1", "type": "Unknown", "description": ""},
            {"name": "c3", "type": "Unknown", "description": ""},
        ]
