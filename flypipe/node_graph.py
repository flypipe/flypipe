from enum import Enum
from typing import List, Union
from matplotlib import pyplot as plt
import networkx as nx
from networkx import DiGraph
from flypipe.node import Node
from flypipe.node_function import NodeFunction
from flypipe.node_run_context import NodeRunContext
from flypipe.output_column_set import OutputColumnSet
from flypipe.utils import DataFrameType


class RunStatus(Enum):
    """Describes the run state of a node in a pipeline when that pipeline is executed"""

    UNKNOWN = 0
    ACTIVE = 1
    SKIP = 2


class NodeGraph:
    """
    Given a transformation node, traverse the transformations the node is dependant upon and build a graph from
    this.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        transformation: Union[Node, None],
        graph=None,
        skipped_node_keys=None,
        pandas_on_spark_use_pandas=False,
        parameters=None,
    ):
        parameters = parameters or {}
        parameters = {node.key: params for node, params in parameters.items()}

        if graph:
            self.graph = graph
        else:
            self.nodes = None
            self.node_output_columns = None
            self.edges = None
            self.graph = self._build_graph(
                transformation, pandas_on_spark_use_pandas, parameters
            )

        if not skipped_node_keys:
            skipped_node_keys = []
        self.skipped_node_keys = skipped_node_keys
        self.calculate_graph_run_status()

    def add_node(  # pylint: disable=too-many-arguments
        self,
        graph: DiGraph,
        node_name: str,
        transformation: Node,
        run_status: RunStatus = None,
        output_columns: list = None,
        run_context: NodeRunContext = None,
    ) -> DiGraph:
        run_status = run_status or RunStatus.UNKNOWN
        run_context = run_context or NodeRunContext()

        graph.add_node(
            node_name,
            transformation=transformation,
            status=run_status,
            output_columns=output_columns,
            run_context=run_context,
        )

        return graph

    def remove_node(self, node_name):
        self.graph.remove_node(node_name)

    def _build_graph(
        self, transformation: Node, pandas_on_spark_use_pandas: bool, parameters: dict
    ):
        transformation = transformation.copy()
        graph = nx.DiGraph()

        frontier = [transformation]
        while frontier:
            current_transformation = frontier.pop()

            node_run_context = NodeRunContext(
                parameters.get(current_transformation.key)
            )
            graph = self.add_node(
                graph,
                current_transformation.key,
                transformation=current_transformation,
                run_context=node_run_context,
            )

            if isinstance(current_transformation, NodeFunction):
                dependencies = current_transformation.node_dependencies
            else:
                dependencies = [
                    input_node.node for input_node in current_transformation.input_nodes
                ]
            for dependency in dependencies:
                frontier.insert(0, dependency)

                graph.add_edge(dependency.key, current_transformation.key)
        graph = self._compute_requested_columns(graph)
        graph = self._expand_node_functions(graph)

        graph = self._compute_edge_selected_columns(graph)

        for node_key in graph.nodes:
            transformation = graph.nodes[node_key]["transformation"]
            # TODO- move this to pandas_on_spark_node once we figure out how to get context to work
            # TODO- create a copy of the node, as in databricks it keeps the objects with type changed until the state
            # is cleared
            if (
                pandas_on_spark_use_pandas
                and transformation.dataframe_type == DataFrameType.PANDAS_ON_SPARK
            ):
                transformation.type = "pandas"

        return graph

    def _compute_edge_selected_columns(self, graph):
        for node_key in graph.nodes:
            node = graph.nodes[node_key]

            if not isinstance(node["transformation"], NodeFunction):
                for input_node in node["transformation"].input_nodes:
                    graph.edges[(input_node.key, node_key)][
                        "selected_columns"
                    ] = input_node.selected_columns

        return graph

    def _expand_node_functions(
        self, graph: DiGraph
    ):  # pylint: disable=too-many-branches
        """
        Expand all node functions. Given a node graph, return the same node graph with all node functions expanded.
        """
        node_functions = [
            graph.nodes[node_key]
            for node_key in graph
            if isinstance(graph.nodes[node_key]["transformation"], NodeFunction)
        ]
        # TODO- pylint flagged the below as having too many nested blocks, we should refactor it to be cleaner
        while node_functions:  # pylint: disable=too-many-nested-blocks
            found_node_function = False
            # FIXME: messy to call this twice
            node_functions = [
                graph.nodes[node_key]
                for node_key in graph
                if isinstance(graph.nodes[node_key]["transformation"], NodeFunction)
            ]
            for node_function in node_functions:
                # We cannot expand a node function until all successor nodes that are node functions have been expanded
                is_runnable_node_function = all(
                    not isinstance(successor["transformation"], NodeFunction)
                    for successor in self._get_successor_nodes(
                        graph, node_function["transformation"].key
                    )
                )
                if is_runnable_node_function:
                    found_node_function = True
                    node_function_key = node_function["transformation"].key

                    expanded_graph = self._expand_node_function(
                        node_function["transformation"],
                        node_function["output_columns"],
                        node_function["run_context"],
                    )

                    # The edges created from the node function node_dependencies are now irrelevant and should be
                    # removed
                    for edge in list(graph.in_edges(node_function_key)):
                        graph.remove_edge(edge[0], edge[1])

                    # Expanded graph can have dependencies to nodes in graph and nodes in graph
                    # can have attributes, such as run_context, that must remain in these nodes when composing a new
                    # graph
                    for expanded_node_key in expanded_graph.nodes:

                        # updates parameters only for those nodes in node function dependencies
                        if expanded_node_key in [
                            dependency.key
                            for dependency in node_function[
                                "transformation"
                            ].node_dependencies
                        ]:
                            if expanded_node_key in graph.nodes:
                                expanded_graph.nodes[expanded_node_key][
                                    "run_context"
                                ] = graph.nodes[expanded_node_key]["run_context"]

                    graph = nx.compose(graph, expanded_graph)

                    # Any successors of the node function need to be repointed to point to the end node that got
                    # returned
                    for node_key in list(graph.nodes):
                        node = graph.nodes[node_key]
                        if isinstance(node["transformation"], NodeFunction):
                            input_nodes = node["transformation"].node_dependencies
                        else:
                            input_nodes = node["transformation"].input_nodes
                        for input_node in input_nodes:
                            if input_node.key == node_function_key:
                                input_node.node = graph.nodes[node_function_key][
                                    "transformation"
                                ]

                    # Recompute the requested columns in the graph as they may have changed after running this
                    self._compute_requested_columns(graph)

                    break

            if not found_node_function and node_functions:
                raise Exception(
                    "Unexpected error- unable to expand all node functions in the graph"
                )

        return graph

    def _expand_node_function(
        self,
        node_function: NodeFunction,
        requested_columns: list,
        run_context: NodeRunContext,
    ):
        """
        Expand a node function in the graph and replace it with the nodes it returns. There are a few additional steps:
        - The end node of the nodes that the node function returns is renamed to have the same key as the node function.
        """

        nodes = node_function.expand(
            requested_columns=requested_columns, parameters=run_context.parameters
        )

        expanded_graph = nx.DiGraph()
        for node in nodes:
            expanded_graph = self.add_node(
                expanded_graph, node.key, transformation=node
            )

        for node in nodes:
            for dependency in node.input_nodes:

                if dependency.key not in expanded_graph.nodes:
                    expanded_graph = self.add_node(
                        expanded_graph, dependency.key, transformation=dependency.node
                    )

                expanded_graph.add_edge(dependency.key, node.key)

        end_node_name = [
            node_name
            for node_name in expanded_graph
            if expanded_graph.out_degree(node_name) == 0
        ][0]
        end_node = expanded_graph.nodes[end_node_name]
        end_node["transformation"].key = node_function.key
        end_node["transformation"].name = node_function.function.__name__
        return nx.relabel_nodes(expanded_graph, {end_node_name: node_function.key})

    def _get_successor_nodes(self, graph, node_key):
        return [graph.nodes[n] for n in graph.successors(node_key)]

    def _compute_requested_columns(self, graph: DiGraph):
        requested_columns = {}
        for node_key in graph:
            node = graph.nodes[node_key]
            if isinstance(node["transformation"], NodeFunction):
                for node_dependency in node["transformation"].node_dependencies:
                    requested_columns[node_dependency.key] = OutputColumnSet(None)
            else:
                for node_input in node["transformation"].input_nodes:
                    if node_input.node.key not in requested_columns:
                        requested_columns[node_input.node.key] = OutputColumnSet(
                            node_input.selected_columns
                        )
                    else:
                        requested_columns[node_input.node.key].add_columns(
                            node_input.selected_columns
                        )

        for node_key, output_columns in requested_columns.items():
            graph.nodes[node_key]["output_columns"] = output_columns.get_columns()

        return graph

    def get_node(self, name: str):
        return self.graph.nodes[name]

    def get_edges(self):
        return self.graph.edges

    def get_edge_data(self, source_node_name, target_node_name):
        return self.graph.get_edge_data(source_node_name, target_node_name)

    def get_transformation(self, name: str) -> Node:
        return self.get_node(name)["transformation"]

    def get_end_node_name(self):
        for name in self.graph:
            if self.graph.out_degree[name] == 0:
                return name
        return None

    def calculate_graph_run_status(self):
        # because the last node can be a generator, we have to get the last node node
        # after building the graph
        node_name = self.get_end_node_name()
        skipped_node_keys = set(self.skipped_node_keys)

        frontier = [
            (
                node_name,
                RunStatus.SKIP if node_name in skipped_node_keys else RunStatus.ACTIVE,
            )
        ]
        while len(frontier) != 0:
            current_node_name, descendent_status = frontier.pop()
            current_node = self.graph.nodes[current_node_name]
            if descendent_status == RunStatus.ACTIVE:
                current_node["status"] = RunStatus.ACTIVE
                for ancestor_name in self.graph.predecessors(current_node_name):
                    if ancestor_name in skipped_node_keys:
                        frontier.append((ancestor_name, RunStatus.SKIP))
                    else:
                        frontier.append((ancestor_name, RunStatus.ACTIVE))
            else:
                current_node["status"] = RunStatus.SKIP
                for ancestor_name in self.graph.predecessors(current_node_name):
                    if self.graph.nodes[ancestor_name]["status"] != RunStatus.ACTIVE:
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
        # - The depth of each node is maximal not minimal, that is if there is a path of length 2 to the start node and
        # a path of length 1 it uses 2 as the depth where it should be 1.
        # - Depth is 1 more than it ought to be. The start node should have depth 0 not depth 1.
        # - We ought to be having the node name as the key and not the value.
        end_node = [
            node_name
            for node_name, num_out_edges in self.graph.out_degree
            if num_out_edges == 0
        ][0]

        nodes_depth = {}
        for node in self.graph:
            depth = len(
                max(
                    list(nx.all_simple_paths(self.graph, node, end_node)),
                    key=lambda x: len(x),  # pylint: disable=unnecessary-lambda
                    default=[end_node],
                )
            )

            if depth not in nodes_depth:
                nodes_depth[depth] = [node]
            else:
                nodes_depth[depth] += [node]

        max_depth = max(nodes_depth.keys())

        return {-1 * k + max_depth + 1: v for k, v in nodes_depth.items()}

    def get_runnable_transformations(self) -> List[Node]:
        candidate_node_names = [
            node_name
            for node_name in self.graph
            if self.graph.in_degree(node_name) == 0
        ]
        runnable_node_names = filter(
            lambda node_name: self.graph.nodes[node_name]["status"] == RunStatus.ACTIVE,
            candidate_node_names,
        )
        runnable_nodes = [self.get_node(node_name) for node_name in runnable_node_names]
        return runnable_nodes

    def is_empty(self):
        return nx.number_of_nodes(self.graph) == 0

    def plot(self, graph=None):
        graph = graph or self.graph

        plt.title("Transformation Graph")
        nx.draw(graph, with_labels=True)
        plt.show()

    def get_execution_graph(self):
        """
        Return an execution graph for this node graph. Practically, this is a copy of the graph with inactive nodes
        filtered out.
        """
        execution_graph = NodeGraph(
            None, graph=self.graph.copy(), skipped_node_keys=self.skipped_node_keys
        )
        to_remove = []
        for node_name in execution_graph.graph.nodes:
            if execution_graph.get_node(node_name)["status"] == RunStatus.SKIP:
                to_remove.append(node_name)
        for node_name in to_remove:
            execution_graph.remove_node(node_name)
        return execution_graph
