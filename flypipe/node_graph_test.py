import pandas as pd

from flypipe.node import Node, node
from flypipe.node_graph import NodeGraph, RunStatus
from flypipe.run_context import RunContext


class TestNodeGraph:
    """Tests for NodeGraph"""

    def test_build_graph(self):
        """
        Ensure an appropriate graph is built for a transformation
           T2
          // \\
        T1    T4
          \\ //
           T3
        """

        @node(type="pandas")
        def t1():
            return

        @node(type="pandas", dependencies=[t1.select("fruit")])
        def t2():
            return

        @node(type="pandas", dependencies=[t1.select("color")])
        def t3():
            return

        @node(type="pandas", dependencies=[t2.select("fruit"), t3.select("color")])
        def t4():
            return

        graph = NodeGraph(t4, run_context=RunContext())
        assert set(graph.get_edges()) == {
            (t1.key, t2.key),
            (t1.key, t3.key),
            (t2.key, t4.key),
            (t3.key, t4.key),
        }
        assert graph.get_edge_data(t1.key, t2.key)["selected_columns"] == ["fruit"]
        assert graph.get_edge_data(t1.key, t3.key)["selected_columns"] == ["color"]
        assert graph.get_node(t1.key)["output_columns"] == ["color", "fruit"]

    def test_calculate_graph_run_status_1(self):
        @node(type="pandas")
        def t1():
            return

        @node(type="pandas")
        def t2():
            return

        @node(type="pandas")
        def t3():
            return

        @node(
            type="pandas",
            dependencies=[t1.select("c1"), t2.select("c1"), t3.select("c1")],
        )
        def t4():
            return

        @node(type="pandas", dependencies=[t2.select("c1"), t3.select("c1")])
        def t5():
            return

        @node(type="pandas", dependencies=[t4.select("c1"), t5.select("c1")])
        def t6():
            return

        graph = NodeGraph(t6, RunContext(provided_inputs={t4: pd.DataFrame()}))

        assert graph.get_node(t1.key)["status"] == RunStatus.SKIP
        assert graph.get_node(t2.key)["status"] == RunStatus.ACTIVE
        assert graph.get_node(t3.key)["status"] == RunStatus.ACTIVE
        assert (
            graph.get_node(t4.key)["status"] == RunStatus.PROVIDED_INPUT
        )  # Changed from SKIP
        assert graph.get_node(t5.key)["status"] == RunStatus.ACTIVE
        assert graph.get_node(t6.key)["status"] == RunStatus.ACTIVE

    def test_calculate_graph_run_status_2(self):
        @node(type="pandas")
        def t1():
            return

        @node(type="pandas")
        def t2():
            return

        @node(type="pandas")
        def t3():
            return

        @node(
            type="pandas",
            dependencies=[t1.select("c1"), t2.select("c1"), t3.select("c1")],
        )
        def t4():
            return

        @node(type="pandas", dependencies=[t2.select("c1"), t3.select("c1")])
        def t5():
            return

        @node(type="pandas", dependencies=[t5.select("c1"), t4.select("c1")])
        def t6():
            return

        graph = NodeGraph(t6, RunContext(provided_inputs={t4: pd.DataFrame()}))

        assert graph.get_node(t1.key)["status"] == RunStatus.SKIP
        assert graph.get_node(t2.key)["status"] == RunStatus.ACTIVE
        assert graph.get_node(t3.key)["status"] == RunStatus.ACTIVE
        assert (
            graph.get_node(t4.key)["status"] == RunStatus.PROVIDED_INPUT
        )  # Changed from SKIP
        assert graph.get_node(t5.key)["status"] == RunStatus.ACTIVE
        assert graph.get_node(t6.key)["status"] == RunStatus.ACTIVE

    def test_get_nodes_depth(self):
        """
        t1 - t3 - t5
           / |
        t2 - t4
        """

        @node(type="pandas")
        def t1():
            return

        @node(type="pandas")
        def t2():
            return

        @node(type="pandas", dependencies=[t1.select("dummy"), t2.select("dummy")])
        def t3():
            return

        @node(type="pandas", dependencies=[t3.select("dummy"), t2.select("dummy")])
        def t4():
            return

        @node(type="pandas", dependencies=[t4.select("dummy")])
        def t5():
            return

        graph = NodeGraph(t5, run_context=RunContext())
        assert graph.get_nodes_depth() == {
            1: [t2.key, t1.key],
            2: [t3.key],
            3: [t4.key],
            4: [t5.key],
        }

    def test_build_graph_diamond_chain_copies_linearly(self, monkeypatch):
        """
        Building the graph must traverse the DAG once per node — O(nodes) — not once
        per root->node path, which is O(2**num_diamonds) on a chain of diamonds. That
        applies to all three traversals: Node.copy (memoized; asserted via the call
        counter), the _build_graph frontier walk (visited set), and
        calculate_graph_run_status (duplicate-stamp guard). At 60 diamonds, 2**60
        paths, any per-path regression hangs this test rather than failing an assert.

            head - left_0 - join_0 - left_1 - join_1 - ...
                 \\ right_0 /     \\ right_1 /
        """
        num_diamonds = 60

        def make_node(name, dependencies):
            def fn(*dfs):
                return

            fn.__name__ = name
            return node(type="pandas", dependencies=dependencies)(fn)

        current = make_node("head", [])
        for i in range(num_diamonds):
            left = make_node(f"left_{i}", [current])
            right = make_node(f"right_{i}", [current])
            current = make_node(f"join_{i}", [left, right])

        calls = {"count": 0}
        original_copy = Node.copy

        def counting_copy(self, _memo=None):
            calls["count"] += 1
            return original_copy(self, _memo)

        monkeypatch.setattr(Node, "copy", counting_copy)
        graph = NodeGraph(current, run_context=RunContext())

        # Memoized copy is called once per edge (memo hits included) plus once for the
        # root: 4 edges per diamond + 1. Unmemoized it is called once per path,
        # 2**(num_diamonds + 2) - 3 times.
        assert calls["count"] <= 4 * num_diamonds + 1

        # The duplicate-stamp guard in calculate_graph_run_status must not swallow
        # any first-time stamp: with no caches or provided inputs, every node is ACTIVE.
        for node_key in graph.graph.nodes:
            assert graph.get_node(node_key)["status"] == RunStatus.ACTIVE

    def test_calculate_graph_run_status_skip_upgraded_to_active_through_diamond(self):
        """
        A node first reached through a non-executing branch (provided input, so its
        ancestors are pushed as SKIP) and later reached through an executing branch
        must end up ACTIVE. The duplicate-stamp guard in calculate_graph_run_status
        only drops pops that re-apply the status a node already has; this locks in
        that pops which *change* a status (SKIP -> ACTIVE here) still process fully.

            t1 - active_branch   - tail
               \\ provided_branch /
        """

        @node(type="pandas")
        def t1():
            return

        @node(type="pandas", dependencies=[t1.select("c1")])
        def active_branch(t1):
            return

        @node(type="pandas", dependencies=[t1.select("c1")])
        def provided_branch(t1):
            return

        @node(
            type="pandas",
            dependencies=[active_branch.select("c1"), provided_branch.select("c1")],
        )
        def tail(active_branch, provided_branch):
            return

        graph = NodeGraph(
            tail,
            run_context=RunContext(provided_inputs={provided_branch: pd.DataFrame()}),
        )

        assert graph.get_node(tail.key)["status"] == RunStatus.ACTIVE
        assert graph.get_node(provided_branch.key)["status"] == RunStatus.PROVIDED_INPUT
        assert graph.get_node(active_branch.key)["status"] == RunStatus.ACTIVE
        # t1 must run for the active branch even though the provided branch skips it
        assert graph.get_node(t1.key)["status"] == RunStatus.ACTIVE
