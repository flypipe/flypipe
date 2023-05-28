from flypipe.cache.cache import Cache
from flypipe.node import node
from flypipe.node_graph import NodeGraph, RunStatus


class TestNodeGraph:
    """Tests for NodeGraph"""

    def test_build_graph(self):
        # pylint: disable=anomalous-backslash-in-string
        """
        Ensure an appropriate graph is built for a transformation
           T2
          /  \
        T1    T4
          \  /
           T3
        """
        # pylint: enable=anomalous-backslash-in-string

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

        graph = NodeGraph(t4)
        assert set(graph.get_edges()) == {
            (t1.key, t2.key),
            (t1.key, t3.key),
            (t2.key, t4.key),
            (t3.key, t4.key),
        }
        assert graph.get_edge_data(t1.key, t2.key)["selected_columns"] == ["fruit"]
        assert graph.get_edge_data(t1.key, t3.key)["selected_columns"] == ["color"]
        assert graph.get_node(t1.key)["output_columns"] == ["color", "fruit"]

    def test_calculate_graph_run_status_skipped_node_1(self):
        """
        Ensure the appropriate calculation of node run statuses takes place when the client provides an input directly
        for a node:
        - The node for which the input (which is really the node output) is supplied should be skipped, since we
        already have the node output there's no need to run.
        - Any ancestor nodes of a skipped node that have no unskipped descendants should also be skipped as they don't
        need to execute.
        """

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

        graph = NodeGraph(t6, inputs={t4.key: None})

        assert graph.get_node(t1.key)["status"] == RunStatus.SKIP
        assert graph.get_node(t2.key)["status"] == RunStatus.ACTIVE
        assert graph.get_node(t3.key)["status"] == RunStatus.ACTIVE
        assert graph.get_node(t4.key)["status"] == RunStatus.SKIP
        assert graph.get_node(t5.key)["status"] == RunStatus.ACTIVE
        assert graph.get_node(t6.key)["status"] == RunStatus.ACTIVE

    def test_calculate_graph_run_status_skipped_node_2(self):
        """
        Due to an implementation quirk we examine the status of the end node in a different way, let's double check if
        it's skipped that it works fine.
        """

        @node(type="pandas")
        def t1():
            return

        @node(type="pandas", dependencies=[t1])
        def t2():
            return

        graph = NodeGraph(t2, inputs={t2.key: None})

        assert graph.get_node(t1.key)["status"] == RunStatus.SKIP
        assert graph.get_node(t2.key)["status"] == RunStatus.SKIP

    def test_calculate_graph_run_status_cached_node(self):
        """
        Nodes with caches that have a cache hit should not be executed. Identical logic to skipped nodes (see
        test_calculate_graph_run_status_skipped_node) except the node with a cache hit has a separate status.
        """

        def dummy_func(context):  # pylint: disable=unused-argument
            return None

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
            cache=Cache(dummy_func, dummy_func, lambda context: True),
        )
        def t4():
            return

        @node(type="pandas", dependencies=[t2.select("c1"), t3.select("c1")])
        def t5():
            return

        @node(
            type="pandas",
            dependencies=[t5.select("c1"), t4.select("c1")],
            cache=Cache(dummy_func, dummy_func, lambda context: False),
        )
        def t6():
            return

        graph = NodeGraph(t6)

        assert graph.get_node(t1.key)["status"] == RunStatus.SKIP
        assert graph.get_node(t2.key)["status"] == RunStatus.ACTIVE
        assert graph.get_node(t3.key)["status"] == RunStatus.ACTIVE
        assert graph.get_node(t4.key)["status"] == RunStatus.CACHED
        assert graph.get_node(t5.key)["status"] == RunStatus.ACTIVE
        assert graph.get_node(t6.key)["status"] == RunStatus.ACTIVE

    def test_calculate_graph_run_status_cached_node_2(self):
        """
        Due to an implementation quirk we examine the status of the end node in a different way, let's double check if
        it's skipped that it works fine.
        """

        def dummy_func(context):  # pylint: disable=unused-argument
            return None

        @node(type="pandas")
        def t1():
            return

        @node(
            type="pandas",
            dependencies=[t1],
            cache=Cache(dummy_func, dummy_func, lambda context: True),
        )
        def t2():
            return

        graph = NodeGraph(t2)

        assert graph.get_node(t1.key)["status"] == RunStatus.SKIP
        assert graph.get_node(t2.key)["status"] == RunStatus.CACHED

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

        graph = NodeGraph(t5)
        assert graph.get_nodes_depth() == {
            1: [t2.key, t1.key],
            2: [t3.key],
            3: [t4.key],
            4: [t5.key],
        }
