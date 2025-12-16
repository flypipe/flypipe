from __future__ import annotations
from typing import Callable, Union, TYPE_CHECKING

from flypipe.run_status import RunStatus

if TYPE_CHECKING:
    from flypipe.node import Node
    from flypipe.node_graph import NodeGraph

from flypipe.dependency import PreprocessMode
from flypipe.dependency.preprocess import Preprocess
from flypipe.run_context import RunContext
from flypipe.utils import get_logger

logger = get_logger()


class InputNode:
    """
    An input node is just a wrapper around a regular node with some extra functionalities on top to allow for usage as
    an input into another node:
    - selected_columns that the receiving node is using.
    - aliasing of the name of the dataframe passed to the receiving node, otherwise a sanitised version of the node
      name is used.
    - static marking to indicate reference data that doesn't require CDC filtering.
    """

    def __init__(self, node: "Node", parent_node: "Node" = None):
        self.node = node
        self._selected_columns = None
        self._alias = None
        self._preprocess = Preprocess()
        self._parent_node = parent_node
        self._static = False

    def set_parent_node(self, parent_node):
        self._parent_node = parent_node

    @property
    def __name__(self):
        return self.node.__name__

    @property
    def key(self):
        return self.node.key

    def get_value(
        self, run_context: RunContext, node_graph: NodeGraph, root_node: Node
    ):
        """
        Retrieve the value of this node input which will be passed to the parent node.

        Parameters
        ----------
        run_context : RunContext
            The run context containing node results
        node_graph : NodeGraph
            The node graph containing node metadata and relationships
        root_node : Node
            The root/target node (for CDC filtering and determining dataframe type)
        """

        logger.debug(f"           ➡ Loading input: {self.node.__name__}")

        # Get node metadata from graph
        cache_context = node_graph.get_cache_context(self.node)
        run_status = node_graph.get_run_status(self.node)

        # Acceptable status Cached, as it is cached and Active, as it is a Cached node with CacheMode.MERGE
        if run_context.has_provided_input(self.node):
            logger.debug(
                f"              📦 Collecting provided input for {self.node.__name__}",
            )
            result = run_context.node_results[self.node][self.node].as_type(
                self._parent_node.dataframe_type
            )
            run_context.update_node_results_with_provided_input(
                self.node, root_node, result.get_df()
            )

        elif cache_context and run_status in [
            RunStatus.CACHED,
            RunStatus.ACTIVE,  # RunStatus.ACTIVE means it is a Cached node with CacheMode.MERGE
        ]:
            result = cache_context.read(
                from_node=self.node, to_node=root_node, is_static=self.static
            )
            run_context.update_node_results(self.node, root_node, result)

        try:
            # We can assume that the computation of the raw node this node input comes from is already done and stored
            # in the run context because it's an ancestor node in the run graph.
            node_input_value = run_context.node_results[self.node][root_node].as_type(
                self._parent_node.dataframe_type
            )
        except KeyError:
            raise RuntimeError(
                f"Unexpected state - unable to find computed result for node {self.key} when used as an input, please "
                f"raise this as a bug in https://github.com/flypipe/flypipe"
            )

        # Preprocess the Input Node
        node_input_value = self.apply_preprocess(
            run_context, self._parent_node, node_input_value
        )

        # Select only necessary columns
        if self.selected_columns:
            node_input_value = node_input_value.select_columns(*self.selected_columns)

        if self._parent_node.type == "spark_sql":
            # SQL doesn't work with dataframes, so we need to:
            # - save all incoming dataframes as unique temporary tables
            # - pass the names of these tables instead of the dataframes
            alias = self.get_alias()
            table_name = f"{self._parent_node.__name__}__{alias}"
            node_input_value.get_df().createOrReplaceTempView(table_name)
            return table_name

        return node_input_value.get_df()

    def set_preprocess(self, *function: Union[PreprocessMode, Callable]):
        self._preprocess.set(*function)
        return self

    def apply_preprocess(
        self, run_context: RunContext, parent_node: "Node", df  # noqa: F821
    ):
        return self._preprocess.apply(run_context, parent_node, self.node, df)

    @property
    def selected_columns(self):
        return self._selected_columns

    def select(self, *columns):
        # TODO- if self.output_schema is defined then we should ensure each of the columns is in it.
        # otherwise if self.output_schema is not defined then we won't know the ultimate output schema
        # so can't do any validation

        cols = columns[0] if isinstance(columns[0], list) else list(columns)

        if len(cols) != len(set(cols)):
            raise ValueError(f"Duplicated columns in selection of {self.__name__}")
        self._selected_columns = sorted(cols)
        return self

    def alias(self, value):
        self._alias = value
        return self

    def set_static(self):
        """
        Mark this InputNode as static.

        When marked as static, CDC (Change Data Capture) filtering will NOT be
        applied after reading from cache. Static nodes are assumed to contain
        reference data that doesn't change across runs.

        Returns
        -------
        InputNode
            Self for method chaining
        """
        self._static = True
        return self

    @property
    def static(self):
        """
        Check if this InputNode is marked as static.

        Static nodes skip CDC filtering when reading from cache, as they are
        assumed to contain reference data that doesn't change.

        Returns
        -------
        bool
            True if static (no CDC filtering), False otherwise
        """
        return self._static

    def get_alias(self):
        if self._alias:
            return self._alias
        # Sometimes the node name will have periods in it, for example if it's coming from a spark table datasource,
        # periods are not valid argument names so let's replace them with underscores.
        return self.__name__.replace(".", "_").replace("<", "").replace(">", "")

    def copy(self):
        # It's necessary to access protected fields to do a deep copy
        # Note: Don't copy _parent_node to avoid infinite recursion
        input_node_copy = InputNode(self.node.copy(), self._parent_node)
        input_node_copy._selected_columns = self._selected_columns
        input_node_copy._alias = self._alias
        input_node_copy._preprocess = self._preprocess.copy()
        input_node_copy._static = self._static
        return input_node_copy
