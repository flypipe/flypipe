from flypipe.node import Node
from flypipe.node_type import NodeType


class NodeFunction(Node):
    """
    Special type of node that returns a series of nodes. Can be used to create a dynamic series of nodes.
    """

    NODE_TYPE = NodeType.NODE_FUNCTION

    def __init__(self, function, node_dependencies=None, requested_columns=False):
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
                        f"node_dependencies must be a list of nodes, found one node dependency of type "
                        f"{type(node_dependency)}"
                    )

    def expand(self, requested_columns: list, parameters: dict = None):
        # TODO- we should not be invoking _key in this function
        kwargs = parameters or {}
        if self.requested_columns:
            kwargs["requested_columns"] = requested_columns

        nodes = self.function(**kwargs)
        if isinstance(nodes, Node):
            nodes = (nodes,)

        set_internal_dependencies = set()
        for node in nodes:
            if isinstance(node, NodeFunction):
                raise ValueError(
                    "Illegal operation - node functions cannot be returned from node functions"
                )
            for dependency in node.input_nodes:
                if dependency not in nodes:
                    set_internal_dependencies.add(dependency.node)

        set_external_dependencies = set(self.node_dependencies)

        difference = set_external_dependencies.difference(set_internal_dependencies)
        if difference:
            difference = [d.__name__ for d in difference]
            raise ValueError(
                f"Some node_dependencies ({', '.join(difference)}) of the node function {self.__name__} "
                f"are not being used by any of its internal nodes"
            )

        difference = set_internal_dependencies.difference(set_external_dependencies)
        if difference:
            difference = [d.__name__ for d in difference]
            raise ValueError(
                f"Some internal nodes dependencies ({', '.join(difference)}) are not declared as "
                f"external dependency of the node function {self.__name__}. "
                f"Please add these nodes to 'node_dependencies' parameter"
            )

        return [node.copy() for node in list(nodes)]

    def copy(self):
        node_function = NodeFunction(
            self.function,
            [dependency.copy() for dependency in self.node_dependencies],
            self.requested_columns,
        )

        node_function._key = self._key
        return node_function


def node_function(*args, **kwargs):
    """
    Decorator factory that returns the given function wrapped inside a NodeFunction class

    Parameters
    ----------

    requested_columns : bool, optional (default `False`)
        List of requested columns that successors nodes are demanding from the node function.
        if True will retrieve `requested_columns` as named argument
    node_dependencies : List[Node or NodeFunction], optional
        List of external nodes that the node function is dependent on.
        Any node retrieved by the node function (called internal node) can only be dependent on any internal node or
        any node inside `node_dependencies`.
        True, returns spark context as argument to the funtion (default is False)

    Returns
    -------
    List[Node]
        a list of nodes created internally

    Raises
    ------
    ValueError
        If any internal node is of type NodeFunction; if any internal node has a dependency that is not to another
        internal node and not declared in node_dependencies


    .. highlight:: python
    .. code-block:: python

        # Syntax
        @node_function(
            requested_columns=True,
            node_dependencies=[
                Spark("table")
            ]
        )
        def my_node_function(requested_columns):

            @node(
                type="pandas",
                dependencies=[
                    Spark("table").select(requested_columns).alias("df")
                ]
            )
            def internal_node_1(df):
                return df


            @node(
                type="pandas",
                dependencies=[
                    internal_node_1.alias("df")
                ]
            )
            def internal_node_2(df):
                return df

            return internal_node_1, internal_node_2 # <-- ALL INTERNAL NODES CREATED MUST BE RETURNED

    """

    def decorator(func):
        return NodeFunction(func, *args, **kwargs)

    return decorator
