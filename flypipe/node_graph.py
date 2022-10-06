import networkx as nx
from enum import Enum
from matplotlib import pyplot as plt


class RunStatus(Enum):
    UNKNOWN = 0
    ACTIVE = 1
    SKIP = 2


class NodeGraph:

    def __init__(self, node, graph=None):
        """
        Given a transformation node, traverse the transformations the node is dependant upon and build a graph from
        this.
        """
        if graph:
            self.graph = graph
        else:
            self.graph = self._build_graph(node)

    def _build_graph(self, node):
        graph = nx.DiGraph()
        # TODO: remove inputs and leave dependencies
        graph.add_node(
            node.__name__,
            name=node.__name__,
            description=node.description,
            tags=node.tags,
            type=node.type,
            transformation=node,
            node_type=node.node_type,
            inputs=[i.__name__ for i in node.dependencies],
            run_status=RunStatus.UNKNOWN,
            output_schema=node.output_schema,
            selected_columns=node.selected_columns
        )

        if node.dependencies:
            for dependency in node.dependencies:
                graph.add_node(dependency.__name__,
                               name=dependency.__name__,
                               description=dependency.description,
                               tags=dependency.tags,
                               type=dependency.type,
                               transformation=dependency,
                               node_type=dependency.node_type,
                               run_status=RunStatus.UNKNOWN,
                               output_schema=dependency.output_schema)
                graph.add_edge(dependency.__name__, node.__name__, selected_columns=node.dependencies_selected_columns[dependency.__name__])
                graph = nx.compose(graph, self._build_graph(dependency))

        return graph

    def get_node(self, name):
        return self.graph.nodes[name]

    def calculate_graph_run_status(self, node_name, skipped_node_names):
        skipped_node_names = set(skipped_node_names)

        frontier = [(node_name, RunStatus.SKIP if node_name in skipped_node_names else RunStatus.ACTIVE)]
        while len(frontier) != 0:
            current_node_name, descendent_run_status = frontier.pop()
            if descendent_run_status == RunStatus.ACTIVE:
                self.get_node(current_node_name)['run_status'] = RunStatus.ACTIVE
                for ancestor_name in self.graph.predecessors(current_node_name):
                    if ancestor_name in skipped_node_names:
                        frontier.append((ancestor_name, RunStatus.SKIP))
                    else:
                        frontier.append((ancestor_name, RunStatus.ACTIVE))
            else:
                self.get_node(current_node_name)['run_status'] = RunStatus.SKIP
                for ancestor_name in self.graph.predecessors(current_node_name):
                    if self.get_node(ancestor_name)['run_status'] != RunStatus.ACTIVE:
                        frontier.append((ancestor_name, RunStatus.SKIP))


    def get_dependency_map(self):
        dependencies = {}
        for node in self.graph.nodes:
            dependencies[node] = set()
        for source, destination in self.graph.edges:
            dependencies[destination].add(source)
        return dependencies

    def pop_runnable_nodes(self):
        candidate_node_names = [node_name for node_name in self.graph if self.graph.in_degree(node_name)==0]
        runnable_node_names = filter(lambda node_name: self.get_node(node_name)['run_status'] == RunStatus.ACTIVE, candidate_node_names)
        runnable_nodes = [self.get_node(node_name) for node_name in runnable_node_names]
        for node_name in candidate_node_names:
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

    def copy(self):
        return NodeGraph(None, graph=self.graph.copy())
