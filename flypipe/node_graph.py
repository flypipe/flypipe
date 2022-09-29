import networkx as nx
from matplotlib import pyplot as plt


class NodeGraph:

    def __init__(self, node, provided_nodes=None):
        """
        Given a transformation node, traverse the transformations the node is dependant upon and build a graph from
        this.
        """
        self.provided_nodes = set(provided_nodes) or set()
        self.graph = self._build_graph(node)

    def _build_graph(self, node):
        graph = nx.DiGraph()
        graph.add_node(
            node.__name__, function=node, inputs=[i.__name__ for i in node.dependencies]
        )

        if node.dependencies:
            for dependency in node.dependencies:
                graph.add_node(dependency.__name__, function=dependency)
                graph.add_edge(dependency.__name__, node.__name__)
                if dependency.__name__ not in self.provided_nodes:
                    graph = nx.compose(graph, self._build_graph(dependency))

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

    def pop_runnable_nodes(self):
        runnable_node_names = [node_name for node_name in self.graph if self.graph.in_degree(node_name)==0]
        runnable_nodes = [self.get_node(node_name) for node_name in runnable_node_names]
        for node_name in runnable_node_names:
            self.graph.remove_node(node_name)
        return runnable_nodes

    def is_empty(self):
        return nx.number_of_nodes(self.graph) == 0

    #
    # @classmethod
    # def get_runnable_nodes(cls, execution_graph):
    #     nodes = [node for node in execution_graph if execution_graph.in_degree(node) == 0]


    def plot(self):
        plt.title(f'Transformation Graph')
        nx.draw(self.graph, with_labels=True)
        plt.show()
