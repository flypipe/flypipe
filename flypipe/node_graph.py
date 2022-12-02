from copy import copy
from enum import Enum
from typing import List

import networkx as nx
from networkx import DiGraph

from flypipe.node import Node
from flypipe.node_function import NodeFunction
from flypipe.node_type import NodeType
from flypipe.output_column_set import OutputColumnSet
from flypipe.utils import DataFrameType


class RunStatus(Enum):
    UNKNOWN = 0
    ACTIVE = 1
    SKIP = 2


class NodeGraph:

    def __init__(self,
                 transformation: Node,
                 graph=None,
                 skipped_node_keys=None,
                 pandas_on_spark_use_pandas=False):
        """
        Given a transformation node, traverse the transformations the node is dependant upon and build a graph from
        this.
        """

        if graph:
            self.graph = graph
        else:
            self.nodes = None
            self.node_output_columns = None
            self.edges = None
            self.graph = self._build_graph(transformation, pandas_on_spark_use_pandas)

        if not skipped_node_keys:
            skipped_node_keys = []
        self.skipped_node_keys = skipped_node_keys
        self.calculate_graph_run_status()

    def _build_graph(self, transformation: Node, pandas_on_spark_use_pandas: bool):
        transformation = transformation.copy()
        graph = nx.DiGraph()

        frontier = [transformation]
        while frontier:
            current_transformation = frontier.pop()

            graph.add_node(
                current_transformation.key,
                transformation=current_transformation,
                status=RunStatus.UNKNOWN,
                output_columns=None,
            )

            if isinstance(current_transformation, NodeFunction):
                dependencies = current_transformation.node_dependencies
            else:
                dependencies = [input_node.node for input_node in current_transformation.input_nodes]
            for dependency in dependencies:
                frontier.insert(0, dependency)

                graph.add_edge(
                    dependency.key,
                    current_transformation.key
                )
        graph = self._compute_requested_columns(graph)
        graph = self._expand_node_functions(graph)

        graph = self._compute_edge_selected_columns(graph)

        # for node_key in graph.nodes:
        #     transformation = graph.nodes[node_key]['transformation']
        #     # TODO- move this to pandas_on_spark_node once we figure out how to get context to work
        #     # TODO- create a copy of the node, as in databricks it keeps the objects with type changed until the state is cleared
        #     if (
        #             pandas_on_spark_use_pandas
        #             and transformation.type == DataFrameType.PANDAS_ON_SPARK
        #     ):
        #         transformation.type = DataFrameType.PANDAS
        #         transformation.original_type = DataFrameType.PANDAS_ON_SPARK
        #
        #     # FIXME: Ticket DATA-3700
        #     elif not pandas_on_spark_use_pandas and hasattr(transformation, "original_type"):
        #         transformation.type = transformation.original_type

        for node_key in graph.nodes:
            transformation = graph.nodes[node_key]['transformation']
            # TODO- move this to pandas_on_spark_node once we figure out how to get context to work
            # TODO- create a copy of the node, as in databricks it keeps the objects with type changed until the state is cleared
            if pandas_on_spark_use_pandas and transformation.type == DataFrameType.PANDAS_ON_SPARK:
                transformation.type = DataFrameType.PANDAS

        return graph

    def _compute_edge_selected_columns(self, graph):
        for node_key in graph.nodes:
            node = graph.nodes[node_key]

            for input_node in node['transformation'].input_nodes:
                graph.edges[(input_node.key, node_key)]['selected_columns'] = input_node.selected_columns

        return graph

    def _expand_node_functions(self, graph: DiGraph):
        """
        Expand all node functions. Given a node graph, return the same node graph with all node functions expanded.
        """
        node_functions = [graph.nodes[node_key] for node_key in graph if
                          isinstance(graph.nodes[node_key]['transformation'], NodeFunction)]
        while node_functions:
            found_node_function = False
            # FIXME: messy to call this twice
            node_functions = [graph.nodes[node_key] for node_key in graph if
                              isinstance(graph.nodes[node_key]['transformation'], NodeFunction)]
            for node_function in node_functions:
                # We cannot expand a node function until all successor nodes that are node functions have been expanded
                is_runnable_node_function = all(
                    [not isinstance(successor['transformation'], NodeFunction)
                     for successor in self._get_successor_nodes(graph, node_function['transformation'].key)]
                )
                if is_runnable_node_function:
                    found_node_function = True
                    node_function_key = node_function['transformation']._key

                    expanded_graph = self._expand_node_function(node_function['transformation'],
                                                                node_function['output_columns'])

                    # The edges created from the node function node_dependencies are now irrelevant and should be removed
                    for edge in list(graph.in_edges(node_function_key)):
                        graph.remove_edge(edge[0], edge[1])

                    graph = nx.compose(graph, expanded_graph)


                    # Any successors of the node function need to be repointed to point to the end node that got returned
                    for node_key in list(graph.nodes):
                        node = graph.nodes[node_key]
                        if isinstance(node['transformation'], NodeFunction):
                            input_nodes = node['transformation'].node_dependencies
                        else:
                            input_nodes = node['transformation'].input_nodes
                        for input_node in input_nodes:
                            if input_node.key == node_function_key:
                                input_node.node = graph.nodes[node_function_key]['transformation']

                    # Recompute the requested columns in the graph as they may have changed after running this
                    self._compute_requested_columns(graph)

                    break

            if not found_node_function and node_functions:
                raise Exception('Unexpected error- unable to expand all node functions in the graph')

        return graph

    def _expand_node_function(self, node_function, requested_columns):
        """
        Expand a node function in the graph and replace it with the nodes it returns. There are a few additional steps:
        - The end node of the nodes that the node function returns is renamed to have the same key as the node function.
        """

        nodes = node_function.expand(requested_columns=requested_columns)

        expanded_graph = nx.DiGraph()
        for node in nodes:
            expanded_graph.add_node(
                node.key,
                transformation=node,
                status=RunStatus.UNKNOWN,
                output_columns=None,
            )

        for node in nodes:
            for dependency in node.input_nodes:

                if dependency.key not in expanded_graph.nodes:

                    expanded_graph.add_node(
                        dependency.key,
                        transformation=dependency.node,
                        status=RunStatus.UNKNOWN,
                        output_columns=None,
                    )

                expanded_graph.add_edge(
                    dependency.key,
                    node.key
                )

        end_node_name = [node_name for node_name in expanded_graph if expanded_graph.out_degree(node_name) == 0][0]
        end_node = expanded_graph.nodes[end_node_name]
        end_node['transformation'].key = node_function._key
        end_node['transformation'].name = node_function.function.__name__
        return nx.relabel_nodes(expanded_graph, {end_node_name: node_function._key})


    def _get_successor_nodes(self, graph, node_key):
        return [graph.nodes[n] for n in graph.successors(node_key)]


    def _compute_requested_columns(self, graph: DiGraph):
        requested_columns = {}
        for node_key in graph:
            node = graph.nodes[node_key]
            if isinstance(node['transformation'], NodeFunction):
                for node_dependency in node['transformation'].node_dependencies:
                    requested_columns[node_dependency.key] = OutputColumnSet(None)
            else:
                for node_input in node['transformation'].input_nodes:
                    if node_input.node.key not in requested_columns:
                        requested_columns[node_input.node.key] = OutputColumnSet(node_input.selected_columns)
                    else:
                        requested_columns[node_input.node.key].add_columns(node_input.selected_columns)

        for node_key, output_columns in requested_columns.items():
            graph.nodes[node_key]['output_columns'] = output_columns.get_columns()

        return graph
    # def _build_graph(self, transformation: Node, pandas_on_spark_use_pandas: bool):
    #     graph = nx.DiGraph()
    #
    #     # Parse the graph, extracting the nodes, edges and selected_columns
    #     self.node_output_columns = {transformation.key: OutputColumnSet(None)}
    #     self.edges = []
    #     frontier = [transformation]
    #     visited = set([transformation.key])
    #     while frontier:
    #         current_transformation = frontier.pop()
    #
    #         if isinstance(current_transformation, NodeFunction):
    #             current_transformation = current_transformation.expand(
    #                 requested_columns=self.node_output_columns[current_transformation.key].get_columns())
    #
    #         # TODO- move this to pandas_on_spark_node once we figure out how to get context to work
    #         # TODO- create a copy of the node, as in databricks it keeps the objects with type changed until the state is cleared
    #         if pandas_on_spark_use_pandas and current_transformation.type==DataFrameType.PANDAS_ON_SPARK:
    #             current_transformation.type = DataFrameType.PANDAS
    #
    #         graph.add_node(
    #             current_transformation.key,
    #             transformation=current_transformation,
    #             output_columns=None,
    #             status=RunStatus.UNKNOWN,
    #         )
    #         for input_node in current_transformation.input_nodes:
    #             if input_node.node.key in self.node_output_columns:
    #                 self.node_output_columns[input_node.node.key].add_columns(input_node.selected_columns)
    #             else:
    #                 self.node_output_columns[input_node.node.key] = OutputColumnSet(input_node.selected_columns)
    #
    #             # At the point where we process the inputs for a node the input node doesn't yet exist in the graph,
    #             # thus we cannot create edges. Instead, we hold a collection of edge data and create the edges after all
    #             # the nodes are added.
    #             self.edges.append((input_node.node.key, current_transformation.key, input_node.selected_columns))
    #             if input_node.node.key not in visited:
    #                 frontier.insert(0, input_node.node)
    #                 visited.add(input_node.node.key)
    #
    #     for node_key, output_columns in self.node_output_columns.items():
    #         graph.nodes[node_key]['output_columns'] = output_columns.get_columns()
    #
    #     for source_node_key, dest_node_key, selected_columns in self.edges:
    #         graph.add_edge(
    #             source_node_key,
    #             dest_node_key,
    #             selected_columns=selected_columns
    #         )
    #     return graph

    def get_node(self, name: str):
        return self.graph.nodes[name]

    def get_edges(self):
        return self.graph.edges

    def get_edge_data(self, source_node_name, target_node_name):
        return self.graph.get_edge_data(source_node_name, target_node_name)

    def get_transformation(self, name: str) -> Node:
        return self.get_node(name)['transformation']

    def get_end_node_name(self):
        for name in self.graph:
            if self.graph.out_degree[name] == 0:
                return name

    def calculate_graph_run_status(self):
        # because the last node can be a generator, we have to get the last node node
        # after building the graph
        node_name = self.get_end_node_name()
        skipped_node_keys = set(self.skipped_node_keys)

        frontier = [(node_name, RunStatus.SKIP if node_name in skipped_node_keys else RunStatus.ACTIVE)]
        while len(frontier) != 0:
            current_node_name, descendent_status = frontier.pop()
            current_node = self.graph.nodes[current_node_name]
            if descendent_status == RunStatus.ACTIVE:
                current_node['status'] = RunStatus.ACTIVE
                for ancestor_name in self.graph.predecessors(current_node_name):
                    if ancestor_name in skipped_node_keys:
                        frontier.append((ancestor_name, RunStatus.SKIP))
                    else:
                        frontier.append((ancestor_name, RunStatus.ACTIVE))
            else:
                current_node['status'] = RunStatus.SKIP
                for ancestor_name in self.graph.predecessors(current_node_name):
                    if self.graph.nodes[ancestor_name]['status'] != RunStatus.ACTIVE:
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
        runnable_node_names = filter(lambda node_name: self.graph.nodes[node_name]['status'] == RunStatus.ACTIVE,
                                     candidate_node_names)
        runnable_nodes = [self.get_node(node_name) for node_name in runnable_node_names]
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
        return NodeGraph(None, graph=self.graph.copy(), skipped_node_keys=self.skipped_node_keys)
