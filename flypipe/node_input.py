class InputNode:
    """
    An input node is just a wrapper around a regular node with some extra functionalities on top to allow for usage as
    an input into another node:
    - selected_columns that the receiving node is using.
    - aliasing of the name of the dataframe passed to the receiving node, otherwise a sanitised version of the node
    name is used.
    """

    def __init__(self, node, selected_columns):
        self.node = node
        self.selected_columns = selected_columns
        self._alias = None

    @property
    def __name__(self):
        return self.node.__name__

    @property
    def key(self):
        return self.node.key

    def get_alias(self):
        if self._alias:
            return self._alias
        # Sometimes the node name will have periods in it, for example if it's coming from a spark table datasource,
        # periods are not valid argument names so let's replace them with underscores.
        return self.__name__.replace('.', '_')

    def alias(self, value):
        self._alias = value
        return self
