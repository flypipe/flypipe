from typing import Union, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from flypipe.run_context import RunContext

from flypipe.dependency.node_input import InputNode
from flypipe.dependency.preprocess_mode import PreprocessMode
from flypipe.utils import get_logger

logger = get_logger()


class NodeDependenciesMixin:
    """
    Mixin class that provides dependency management methods for Node.

    This class handles input nodes, preprocessing, column selection, and aliasing
    for node dependencies.
    """

    def _get_input_nodes(self, dependencies):
        """
        Process and validate dependencies, converting them to InputNode objects.

        Parameters
        ----------
        dependencies : List
            List of node dependencies (either Node or InputNode objects)

        Returns
        -------
        List[InputNode]
            List of validated InputNode objects

        Raises
        ------
        ValueError
            If duplicate nodes/aliases are detected or invalid dependency types
        """
        input_nodes = []
        input_node_keys = set()
        input_node_alias = set()
        if dependencies is None:
            dependencies = []
        for dependency in dependencies:
            if dependency.key in input_node_keys:
                raise ValueError(
                    f"Illegal operation- node {self.__name__} is using the same node {dependency.__name__} more than "
                    f"once"
                )

            # Import Node here to avoid circular import
            from flypipe.node import Node

            if isinstance(dependency, Node):
                input_node = InputNode(dependency, parent_node=self)
                input_nodes.append(input_node)
            elif isinstance(dependency, InputNode):
                input_node = dependency
                input_node.set_parent_node(self)
                input_nodes.append(input_node)
            else:
                raise ValueError(
                    f"Expected all dependencies of node {self.__name__} to be of format node/node.alias(...)/node."
                    f"select(...) but received {dependency} of type {type(dependency)}"
                )

            if input_node.get_alias() in input_node_alias:
                raise ValueError(
                    f"Illegal operation- node {self.__name__} has multiple nodes with the same name/alias"
                )
            input_node_keys.add(input_node.key)
            input_node_alias.add(input_node.get_alias())
        return input_nodes

    def preprocess(self, *arg: Union[PreprocessMode, Callable]) -> InputNode:
        """
        Create an InputNode with preprocessing configuration.

        Parameters
        ----------
        *arg : Union[PreprocessMode, Callable]
            Preprocessing mode or custom preprocessing function

        Returns
        -------
        InputNode
            InputNode with preprocessing configuration applied
        """
        return InputNode(self).set_preprocess(*arg)

    def select(self, *columns):
        """
        Create an InputNode with column selection.

        Parameters
        ----------
        *columns : str
            Column names to select from this node

        Returns
        -------
        InputNode
            InputNode with column selection applied
        """
        return InputNode(self).select(*columns)

    def alias(self, value):
        """
        Create an InputNode with an alias.

        Parameters
        ----------
        value : str
            Alias name for the node

        Returns
        -------
        InputNode
            InputNode with alias applied
        """
        return InputNode(self).alias(value)

    def static(self):
        """
        Mark this node as static, meaning its result won't change across runs.

        Static nodes are cached and reused without re-execution. When a node is
        marked as static, CDC (Change Data Capture) filtering will NOT be applied
        after reading from cache, as static nodes are assumed to contain reference
        data that doesn't change.

        This is useful for nodes that load reference data (lookup tables, configuration)
        or perform expensive computations that don't need to be recalculated every time.

        Returns
        -------
        InputNode
            InputNode marked as static
        """
        return InputNode(self).set_static()

    def get_node_inputs(self, run_context: "RunContext", node_graph, target_node=None):
        """
        Get the input values for this node from its dependencies.

        Parameters
        ----------
        run_context : RunContext
            The runtime context containing node results and configuration
        node_graph : NodeGraph
            The execution graph containing node metadata
        target_node : Node, optional
            The target node for CDC filtering (defaults to self)

        Returns
        -------
        dict
            Dictionary mapping input aliases to their computed values
        """
        inputs = {}
        for input_node in self.input_nodes:
            # Pass target_node as root_node for CDC filtering (use self as fallback for backwards compatibility)
            inputs[input_node.get_alias()] = input_node.get_value(
                run_context, node_graph, target_node or self
            )
        if not self.input_nodes:
            logger.debug(f"           └─ {self.__name__} has no predecessors")
        return inputs
