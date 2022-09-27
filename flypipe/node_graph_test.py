from flypipe.node import node
from flypipe.node_graph import NodeGraph


@node(type="pandas")
def a():
    return


@node(type="pandas", inputs=[a])
def b():
    return


@node(type="pandas", inputs=[a])
def c():
    return


@node(type="pandas", inputs=[b])
def d():
    return


@node(type="pandas", inputs=[b])
def e():
    return


@node(type="pandas", inputs=[a, c])
def f():
    return


@node(type="pandas", inputs=[d, e, f])
def g():
    return


class TestNodeGraph:

    def test_get_dependency_map(self):
        graph = NodeGraph(g)
        assert graph.get_dependency_map() == {
            "a": set(),
            "b": {"a"},
            "c": {"a"},
            "d": {"b"},
            "e": {"b"},
            "f": {"a", "c"},
            "g": {"d", "e", "f"},
        }
