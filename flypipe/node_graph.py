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
            node.__name__, function=node, inputs=[i.__name__ for i in node.inputs]
        )

        if node.inputs:
            for input in node.inputs:
                graph.add_node(input.__name__, function=input)
                graph.add_edge(input.__name__, node.__name__)
                graph = nx.compose(graph, cls._build_graph(input))

        return graph

    def get_node(self, name):
        return self.graph.nodes[name]['function']

    def get_dependency_chain(self):
        """
        Process the graph to get the appropriate order to execute the nodes in.
        """
        dependency_chain = []
        graph = self.graph.copy()

        while not len(graph) == 0:
            nodes = [node for node in graph if graph.in_degree(node)==0]
            dependency_chain.append(nodes)
            for node in nodes:
                graph.remove_node(node)

        return dependency_chain

    def plot(self):
        plt.title(f'Transformation Graph')
        nx.draw(self.graph, with_labels=True)
        plt.show()
