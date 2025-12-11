from __future__ import annotations
from typing import Callable, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from flypipe.node import Node

from flypipe.dependency import PreprocessMode
from flypipe.dependency.preprocess import Preprocess
from flypipe.run_context import RunContext


class InputNode:
    """
    An input node is just a wrapper around a regular node with some extra functionalities on top to allow for usage as
    an input into another node:
    - selected_columns that the receiving node is using.
    - aliasing of the name of the dataframe passed to the receiving node, otherwise a sanitised version of the node
    name is used.
    """

    def __init__(self, node: Node, parent_node: Node = None):
        self.node = node
        self._selected_columns = None
        self._alias = None
        self._preprocess = Preprocess()
        self._parent_node = parent_node

    def set_parent_node(self, parent_node):
        self._parent_node = parent_node

    @property
    def __name__(self):
        return self.node.__name__

    @property
    def key(self):
        return self.node.key

    def get_value(self, run_context: RunContext, root_node: Node):
        """
        Retrieve the value of this node input which will be passed to the parent node.

        Parameters
        ----------
        run_context : RunContext
            The run context containing node results
        root_node : Node
            The root/target node (for CDC filtering and determining dataframe type)
        """

        try:
            # We can assume that the computation of the raw node this node input comes from is already done and stored
            # in the run context because it's an ancestor node in the run graph.
            node_input_value = run_context.node_results[self.key].as_type(
                self._parent_node.dataframe_type
            )
        except KeyError:
            raise RuntimeError(
                f"Unexpected state- unable to find computed result for node {self.key} when used as an input, please "
                f"raise this as a bug in https://github.com/flypipe/flypipe"
            )

        cache_context = run_context.get_cache_context(self.node)

        # In cases that dataframe is provided as input, there might not be any CacheContext created
        # and we need to check if the cache context has cache.
        if cache_context:
            df = cache_context.read_cdc(self.node, root_node, node_input_value.get_df())
            node_input_value = node_input_value.clone(df)

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
        return input_node_copy
