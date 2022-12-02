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
            if isinstance(node, NodeFunction):
                raise ValueError('Illegal operation- node functions cannot be returned from node functions')
            for dependency in node.input_nodes:
                if dependency not in nodes and dependency not in self.node_dependencies:
                    raise ValueError(
                        f'Unknown node {dependency.key} in node function {self._key} dependencies {[n._key for n in self.node_dependencies]}, all external dependencies must '
                        f'be defined in node function parameter node_dependencies')

        return list(nodes)

    def copy(self):
        node_function = NodeFunction(
            self.function,
            [dependency.copy() for dependency in self.node_dependencies],
            self.requested_columns
        )

        node_function._key = self._key
        return node_function


def node_function(*args, **kwargs):
    """
    Decorator factory that returns the given function wrapped inside a Node class
    """

    def decorator(func):
        return NodeFunction(func, *args, **kwargs)

    return decorator
