from flypipe.node import node
from flypipe.node_graph import NodeGraph, RunStatus


class TestNodeGraph:
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

        @node(type="pandas", dependencies=[t1, t2, t3])
        def t4():
            return

        @node(type="pandas", dependencies=[t2, t3])
        def t5():
            return

        @node(type="pandas", dependencies=[t4, t5])
        def t6():
            return

        graph = NodeGraph(t6)
        graph.calculate_graph_run_status("t6", ["t4"])

        assert graph.get_node_run_status("t1") == RunStatus.SKIP
        assert graph.get_node_run_status("t2") == RunStatus.ACTIVE
        assert graph.get_node_run_status("t3") == RunStatus.ACTIVE
        assert graph.get_node_run_status("t4") == RunStatus.SKIP
        assert graph.get_node_run_status("t5") == RunStatus.ACTIVE
        assert graph.get_node_run_status("t6") == RunStatus.ACTIVE

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

        @node(type="pandas", dependencies=[t1, t2, t3])
        def t4():
            return

        @node(type="pandas", dependencies=[t2, t3])
        def t5():
            return

        @node(type="pandas", dependencies=[t5, t4])
        def t6():
            return

        graph = NodeGraph(t6)
        graph.calculate_graph_run_status("t6", ["t4"])

        assert graph.get_node_run_status("t1") == RunStatus.SKIP
        assert graph.get_node_run_status("t2") == RunStatus.ACTIVE
        assert graph.get_node_run_status("t3") == RunStatus.ACTIVE
        assert graph.get_node_run_status("t4") == RunStatus.SKIP
        assert graph.get_node_run_status("t5") == RunStatus.ACTIVE
        assert graph.get_node_run_status("t6") == RunStatus.ACTIVE
