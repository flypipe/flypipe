from flypipe.node import Node
from flypipe.node_type import NodeType


class NodeFunction(Node):
    NODE_TYPE = NodeType.NODE_FUNCTION

    def __init__(
        self,
        function,
        node_dependencies=None,
        requested_columns=False
    ):
        self._key = None
        self.function = function
        self.node_dependencies = node_dependencies or []
        self._validate_node_dependencies()
        self.requested_columns = requested_columns

    def _validate_node_dependencies(self):
        if self.node_dependencies:
            for node_dependency in self.node_dependencies:
                if not isinstance(node_dependency, Node):
                    raise TypeError(
                        f'node_dependencies must be a list of nodes, found one node dependency of type '
                        f'{type(node_dependency)}')

    def expand(self, requested_columns):
        kwargs = {}
        if self.requested_columns:
            kwargs['requested_columns'] = requested_columns
        nodes = self.function(**kwargs)
        if isinstance(nodes, Node):
            nodes = (nodes,)
        # nodes[-1].key = self.key

        for node in nodes:
            for dependency in node.input_nodes:
                if dependency not in nodes and dependency not in self.node_dependencies:
                    raise ValueError(
                        f'Unknown node {dependency.key} in node function {self._key}, all external dependencies must '
                        f'be defined in node function parameter node_dependencies')

        return list(nodes)

    def run(self, spark=None, parallel=None, inputs=None, pandas_on_spark_use_pandas=False):
        # TODO: would be nice not to have to copy paste this code from Node.run
        if not inputs:
            inputs = {}
        provided_inputs = {node.key: df for node, df in inputs.items()}
        self._create_graph(list(provided_inputs.keys()), pandas_on_spark_use_pandas)
        requested_columns = self.node_graph.node_output_columns[self.key].get_columns()
        return self.expand(requested_columns).run(spark, parallel, inputs, pandas_on_spark_use_pandas)

    def copy(self):
        return NodeFunction(
            self.function,
            [dependency.copy() for dependency in self.node_dependencies],
            self.requested_columns
        )


def node_function(*args, **kwargs):
    """
    Decorator factory that returns the given function wrapped inside a Node class
    """

    def decorator(func):
        return NodeFunction(func, *args, **kwargs)

    return decorator
