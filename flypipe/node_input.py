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

    @property
    def __name__(self):
        return self.node.__name__

    def alias(self, alias):
        # TODO- implement
        pass
