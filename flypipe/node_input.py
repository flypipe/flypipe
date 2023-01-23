class InputNode:
    """
    An input node is just a wrapper around a regular node with some extra functionalities on top to allow for usage as
    an input into another node:
    - selected_columns that the receiving node is using.
    - aliasing of the name of the dataframe passed to the receiving node, otherwise a sanitised version of the node
    name is used.
    """

    def __init__(self, node):
        self.node = node
        self._selected_columns = None
        self._alias = None

    @property
    def __name__(self):
        return self.node.__name__

    @property
    def key(self):
        return self.node.key

    def select(self, *columns):
        # TODO- if self.output_schema is defined then we should ensure each of the columns is in it.
        # otherwise if self.output_schema is not defined then we won't know the ultimate output schema
        # so can't do any validation

        cols = columns[0] if isinstance(columns[0], list) else list(columns)

        if len(cols) != len(set(cols)):
            raise ValueError(f"Duplicated columns in selection of {self.__name__}")
        self._selected_columns = sorted(cols)
        return self

    @property
    def selected_columns(self):
        return self._selected_columns

    def get_alias(self):
        if self._alias:
            return self._alias
        # Sometimes the node name will have periods in it, for example if it's coming from a spark table datasource,
        # periods are not valid argument names so let's replace them with underscores.
        return self.__name__.replace(".", "_").replace("<", "").replace(">", "")

    def alias(self, value):
        self._alias = value
        return self

    def copy(self):
        # It's necessary to access protected fields to do a deep copy
        # pylint: disable=protected-access
        input_node_copy = InputNode(self.node.copy())
        input_node_copy._selected_columns = self._selected_columns
        input_node_copy._alias = self._alias
        # pylint: enable=protected-access
        return input_node_copy
