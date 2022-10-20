import pytest

from flypipe.data_type import String
from flypipe.datasource.spark import Spark
from flypipe.node import node
from flypipe.node_graph import NodeGraph, RunStatus
from flypipe.schema import Schema, Column


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark

    (
        spark
            .createDataFrame(schema=('t1.c1', 't1.c2', 't1.c3'), data=[(1, 2, 3)])
            .createOrReplaceTempView('t1')

    )

    return spark

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

        @node(type="pandas", dependencies=[t1.select('c1'),
                                           t2.select('c1'),
                                           t3.select('c1')])
        def t4():
            return

        @node(type="pandas", dependencies=[t2.select('c1'),
                                           t3.select('c1')])
        def t5():
            return

        @node(type="pandas", dependencies=[t4.select('c1'),
                                           t5.select('c1')])
        def t6():
            return

        graph = NodeGraph(t6)
        graph.calculate_graph_run_status("t6", ["t4"])

        assert graph.get_node("t1")['run_status'] == RunStatus.SKIP
        assert graph.get_node("t2")['run_status'] == RunStatus.ACTIVE
        assert graph.get_node("t3")['run_status'] == RunStatus.ACTIVE
        assert graph.get_node("t4")['run_status'] == RunStatus.SKIP
        assert graph.get_node("t5")['run_status'] == RunStatus.ACTIVE
        assert graph.get_node("t6")['run_status'] == RunStatus.ACTIVE

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

        @node(type="pandas", dependencies=[t1.select('c1'),
                                           t2.select('c1'),
                                           t3.select('c1')])
        def t4():
            return

        @node(type="pandas", dependencies=[t2.select('c1'),
                                           t3.select('c1')])
        def t5():
            return

        @node(type="pandas", dependencies=[t5.select('c1'),
                                           t4.select('c1')])
        def t6():
            return

        graph = NodeGraph(t6)
        graph.calculate_graph_run_status("t6", ["t4"])

        assert graph.get_node("t1")['run_status']==RunStatus.SKIP
        assert graph.get_node("t2")['run_status']==RunStatus.ACTIVE
        assert graph.get_node("t3")['run_status']==RunStatus.ACTIVE
        assert graph.get_node("t4")['run_status']==RunStatus.SKIP
        assert graph.get_node("t5")['run_status']==RunStatus.ACTIVE
        assert graph.get_node("t6")['run_status']==RunStatus.ACTIVE

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

        @node(type="pandas", dependencies=[t1.select('dummy'), t2.select('dummy')])
        def t3():
            return

        @node(type="pandas", dependencies=[t3.select('dummy'), t2.select('dummy')])
        def t4():
            return

        @node(type="pandas", dependencies=[t4.select('dummy')])
        def t5():
            return

        graph = NodeGraph(t5)
        assert graph.get_nodes_depth() == {1: ['t2', 't1'], 2: ['t3'], 3: ['t4'], 4: ['t5']}

    def test_build_graph(self):

        @node(type="pandas")
        def t1():
            return
        graph = NodeGraph(t1)
        assert not graph.graph.nodes['t1']['graph_selected_columns']
        assert not t1.selected_columns
        assert not t1.dependencies_selected_columns

        @node(type="pandas",
              dependencies=[
                  t1.select('t1.col1', 't1.col2')
              ],
              output=Schema([
                  Column("t2.col1", String(), "dummy"),
                  Column("t2.col2", String(), "dummy"),
                  Column("t2.col3", String(), "dummy"),
                  Column("t2.col4", String(), "dummy"),
              ])
        )
        def t2():
            return

        graph = NodeGraph(t2)
        assert graph.graph.nodes['t1']['graph_selected_columns'] == ['t1.col1', 't1.col2']
        assert not graph.graph.nodes['t2']['graph_selected_columns']

        assert t1.selected_columns == ['t1.col1', 't1.col2']
        assert not t1.dependencies_selected_columns

        assert not t2.selected_columns
        assert t2.dependencies_selected_columns == {'t1': ['t1.col1', 't1.col2']}

        @node(type="pandas",
              dependencies=[
                  t1.select('t1.col1', 't1.col3'),
                  t2.select('t2.col2'),
              ]
        )
        def t3():
            return

        graph = NodeGraph(t3)
        assert graph.graph.nodes['t1']['graph_selected_columns'].sort() == ['t1.col1', 't1.col2', 't1.col3'].sort()
        assert graph.graph.nodes['t2']['graph_selected_columns'] == ['t2.col2']
        assert not graph.graph.nodes['t3']['graph_selected_columns']

        assert t1.selected_columns == ['t1.col1', 't1.col3']
        assert not t1.dependencies_selected_columns

        assert t2.selected_columns == ['t2.col2']
        assert t2.dependencies_selected_columns == {'t1': ['t1.col1', 't1.col2']}

        assert not t3.selected_columns
        assert t3.dependencies_selected_columns == {'t1': ['t1.col1', 't1.col3'], 't2': ['t2.col2']}

    def test_build_graph_with_datasource(self):

        @node(type="pandas",
              dependencies=[
                  Spark("t1").select('t1.col1', 't1.col2')
              ],
              output=Schema([
                  Column("t2.col1", String(), "dummy"),
                  Column("t2.col2", String(), "dummy"),
                  Column("t2.col3", String(), "dummy"),
                  Column("t2.col4", String(), "dummy"),
              ])
        )
        def t2():
            return

        graph = NodeGraph(t2)
        assert graph.graph.nodes['t1']['graph_selected_columns'] == ['t1.col1', 't1.col2']
        assert not graph.graph.nodes['t2']['graph_selected_columns']


        assert graph.graph.nodes['t1']['transformation'].selected_columns == ['t1.col1', 't1.col2']
        assert not graph.graph.nodes['t1']['transformation'].dependencies_selected_columns

        assert not t2.selected_columns
        assert t2.dependencies_selected_columns == {'t1': ['t1.col1', 't1.col2']}

        @node(type="pandas",
              dependencies=[
                  Spark("t1").select('t1.col1', 't1.col3'),
                  t2.select('t2.col2'),
              ]
        )
        def t3():
            return

        graph = NodeGraph(t3)
        assert graph.graph.nodes['t1']['graph_selected_columns'].sort() == ['t1.col1', 't1.col2', 't1.col3'].sort()
        assert graph.graph.nodes['t2']['graph_selected_columns'] == ['t2.col2']
        assert not graph.graph.nodes['t3']['graph_selected_columns']

        assert graph.graph.nodes['t1']['transformation'].selected_columns == ['t1.col1', 't1.col2']
        assert not graph.graph.nodes['t1']['transformation'].dependencies_selected_columns

        assert t2.selected_columns == ['t2.col2']
        assert t2.dependencies_selected_columns == {'t1': ['t1.col1', 't1.col2']}

        assert not t3.selected_columns
        assert t3.dependencies_selected_columns == {'t1': ['t1.col1', 't1.col3'], 't2': ['t2.col2']}

