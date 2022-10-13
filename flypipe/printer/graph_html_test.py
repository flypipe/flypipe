from flypipe import node
from flypipe.printer.graph_html import GraphHTML


class TestNodeGraph:
    def test_calculate_graph_run_status_1(self):
        @node(type="pandas")
        def t1():
            return


        nodes_position = GraphHTML._nodes_position()

    def test_get_node_positions_1(self):
        @node(type='pandas')
        def t1():
            return

        @node(type='pandas', dependencies=[t1.select('dummy')])
        def t2():
            return

        @node(type='pandas', dependencies=[t2.select('dummy')])
        def t3():
            return

        # TODO- we should not be having to call a private method to setup
        t3._create_graph()
        positions = GraphHTML(t3.node_graph).get_node_positions()
        assert positions == {'t1': [1.0, 50.0], 't2': [2.0, 47.5], 't3': [3.0, 50.0]}

    def test_get_node_positions_2(self):
        @node(type='pandas')
        def t1():
            return

        @node(type='pandas', dependencies=[t1.select('dummy')])
        def t2():
            return

        @node(type='pandas', dependencies=[t1.select('dummy')])
        def t3():
            return

        @node(type='pandas', dependencies=[t2.select('dummy'), t3.select('dummy')])
        def t4():
            return

        # TODO- we should not be having to call a private method to setup
        t4._create_graph()
        positions = GraphHTML(t4.node_graph).get_node_positions()
        assert positions == {'t1': [1.0, 50.0], 't2': [2.0, 30.83], 't3': [2.0, 64.16], 't4': [3.0, 50.0]}
