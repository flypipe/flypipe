from flypipe import node
from flypipe.printer.graph_html import GraphHTML


class TestNodeGraph:
    def test_calculate_graph_run_status_1(self):
        @node(type="pandas")
        def t1():
            return


        nodes_position = GraphHTML._nodes_position()
