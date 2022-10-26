from enum import Enum
from typing import List

import networkx as nx

from flypipe.node import Node
from flypipe.utils import DataFrameType


class RunStatus(Enum):
    UNKNOWN = 0
    ACTIVE = 1
    SKIP = 2




class NodeGraph:

    def __init__(self, transformation: Node, graph=None, pandas_on_spark_use_pandas=False):
        """
        Given a transformation node, traverse the transformations the node is dependant upon and build a graph from
        this.
        """
        if graph:
            self.graph = graph
        else:
            self.graph = self._build_graph(transformation, pandas_on_spark_use_pandas)

    def __repr__(self):
        graph_str = ""
        for node in self.graph:
            graph_str += "\n" + str(self.get_transformation(node))

        return graph_str

    def _build_graph(self, transformation: Node, pandas_on_spark_use_pandas: bool) -> nx.DiGraph:
        graph = nx.DiGraph()

        # TODO- move this to pandas_on_spark_node once we figure out how to get context to work
        if pandas_on_spark_use_pandas and transformation.type == DataFrameType.PANDAS_ON_SPARK:
            transformation.type = DataFrameType.PANDAS
        graph.add_node(
            transformation.__name__,
            transformation=transformation,
            run_status=RunStatus.UNKNOWN,
            output_columns=None,
        )

        frontier = [transformation]
        while frontier:
            current_transformation = frontier.pop()

            if current_transformation.input_nodes:
                for input_node in current_transformation.input_nodes:
                    if input_node.__name__ not in graph.nodes:
                        graph.add_node(
                            input_node.__name__,
                            transformation=input_node.node,
                            run_status=RunStatus.UNKNOWN,
                            output_columns=list(input_node.selected_columns),
                        )
                    else:
                        graph.nodes[input_node.__name__]['output_columns'].extend(input_node.selected_columns)
                    graph.add_edge(
                        input_node.__name__,
                        current_transformation.__name__,
                        selected_columns=input_node.selected_columns
                    )
                    frontier.insert(0, input_node.node)
        return graph

    def get_node(self, name: str):
        return self.graph.nodes[name]

    def get_node_output_columns(self, name: str):
        return self.graph.nodes[name]['output_columns']

    def get_edges(self):
        return self.graph.edges

    def get_edge_data(self, source_node_name, target_node_name):
        return self.graph.get_edge_data(source_node_name, target_node_name)

    def get_transformation(self, name: str) -> Node:
        return self.get_node(name)['transformation']

    def get_run_status(self, name: str) -> RunStatus:
        return self.get_node(name)['run_status']

    def set_run_status(self, name: str, run_status: RunStatus):
        self.graph.nodes[name]['run_status'] = run_status

    def calculate_graph_run_status(self, node_name, skipped_node_names):
        skipped_node_names = set(skipped_node_names)

        frontier = [(node_name, RunStatus.SKIP if node_name in skipped_node_names else RunStatus.ACTIVE)]
        while len(frontier) != 0:
            current_node_name, descendent_run_status = frontier.pop()
            if descendent_run_status == RunStatus.ACTIVE:
                self.set_run_status(current_node_name, RunStatus.ACTIVE)
                for ancestor_name in self.graph.predecessors(current_node_name):
                    if ancestor_name in skipped_node_names:
                        frontier.append((ancestor_name, RunStatus.SKIP))
                    else:
                        frontier.append((ancestor_name, RunStatus.ACTIVE))
            else:
                self.set_run_status(current_node_name, RunStatus.SKIP)
                for ancestor_name in self.graph.predecessors(current_node_name):
                    if self.get_run_status(ancestor_name) != RunStatus.ACTIVE:
                        frontier.append((ancestor_name, RunStatus.SKIP))


    def get_dependency_map(self):
        dependencies = {}
        for node in self.graph.nodes:
            dependencies[node] = set()
        for source, destination in self.graph.edges:
            dependencies[destination].add(source)
        return dependencies

    def get_nodes_depth(self):
        """
        Return a map of node names to their depth in the graph, depth being the minimal distance to the root/first node.
        """
        # TODO- this function is failing unit tests, I can see 3 issues:
        # - The depth of each node is maximal not minimal, that is if there is a path of length 2 to the start node and a
        # path of length 1 it uses 2 as the depth where it should be 1.
        # - Depth is 1 more than it ought to be. The start node should have depth 0 not depth 1.
        # - We ought to be having the node name as the key and not the value.
        end_node = [node_name for node_name, num_out_edges in self.graph.out_degree if num_out_edges==0][0]

        nodes_depth = {}
        for node in self.graph:
            depth = len(
                max(list(nx.all_simple_paths(self.graph, node, end_node)), key=lambda x: len(x), default=[end_node]))

            if depth not in nodes_depth:
                nodes_depth[depth] = [node]
            else:
                nodes_depth[depth] += [node]

        max_depth = max(nodes_depth.keys())

        return {-1 * k + max_depth + 1: v for k, v in nodes_depth.items()}

    def pop_runnable_transformations(self) -> List[Node]:
        candidate_node_names = [node_name for node_name in self.graph if self.graph.in_degree(node_name)==0]
        runnable_node_names = filter(lambda node_name: self.get_run_status(node_name) == RunStatus.ACTIVE,
                                     candidate_node_names)
        runnable_nodes = [self.get_transformation(node_name) for node_name in runnable_node_names]
        for node_name in candidate_node_names:
            self.graph.remove_node(node_name)
        return runnable_nodes

    def is_empty(self):
        return nx.number_of_nodes(self.graph) == 0

    def plot(self):

        from matplotlib import pyplot as plt

        plt.title(f'Transformation Graph')
        nx.draw(self.graph, with_labels=True)
        plt.show()

    def copy(self):
        return NodeGraph(None, graph=self.graph.copy())
