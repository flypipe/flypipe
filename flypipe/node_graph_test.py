from flypipe.node import node
from flypipe.node_graph import NodeGraph


@node()
def a():
    return


@node(inputs=[a])
def b():
    return


@node(inputs=[a])
def c():
    return


@node(inputs=[b])
def d():
    return


@node(inputs=[b])
def e():
    return


@node(inputs=[a, c])
def f():
    return


@node(inputs=[d, e, f])
def g():
    return


class TestNodeGraph:

    def test_get_dependency_chain(self):
        graph = NodeGraph(g)
        assert graph.get_dependency_chain() == [['a'], ['b', 'c'], ['d', 'e', 'f'], ['g']]
