import networkx as nx
from matplotlib import pyplot as plt


class NodeGraph:

    def __init__(self, node):
        """
        Given a transformation node, traverse the transformations the node is dependant upon and build a graph from
        this.
        """
        self.graph = self._build_graph(node)

    @classmethod
    def _build_graph(cls, node):
        graph = nx.DiGraph()
        graph.add_node(
            node.__name__, function=node, inputs=[i.node.__name__ for i in node.inputs]
        )

        if node.inputs:
            for input in node.inputs:
                graph.add_node(input.node.__name__, function=input)
                graph.add_edge(input.node.__name__, node.__name__)
                graph = nx.compose(graph, cls._build_graph(input.node))

        return graph

    def get_node(self, name):
        return self.graph.nodes[name]['function']

    def get_dependency_map(self):
        dependencies = {}
        for node in self.graph.nodes:
            dependencies[node] = set()
        for source, destination in self.graph.edges:
            dependencies[destination].add(source)
        return dependencies
    #
    # @classmethod
    # def get_runnable_nodes(cls, execution_graph):
    #     nodes = [node for node in execution_graph if execution_graph.in_degree(node) == 0]


    def plot(self):
        plt.title(f'Transformation Graph')
        nx.draw(self.graph, with_labels=True)
        plt.show()
