from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flypipe.node import Node


class Schema:
    """
    Holds information about the nature of the output dataframe from a Flypipe transformation.
    """

    def __init__(self, *columns):
        self.columns = columns[0] if isinstance(columns[0], list) else list(columns)

        """
        this easy the access to a column of an output, for example:
        
        @node(
            ...
            output=Schema(
                t1.output.get("col1") OR t1.output.col1 
            )
        )
        """
        for col in self.columns:
            setattr(self, col.name, col)

    def set_parents(self, parent: "Node"):
        for col in self.columns:
            col._set_parent(parent)

    def get(self, column: str):
        for col in self.columns:
            if col.name == column:
                return col
        return None

    def get_column_names(self):
        return [column.name for column in self.columns]

    def __repr__(self):
        if not self.columns:
            return "Schema: no columns"

        cols = [str(col) for col in self.columns]
        return "Schema([" + ", ".join(cols) + "])"

    def copy(self):
        return Schema([column.copy() for column in self.columns])

    def __eq__(self, other):
        is_equal = len(self.columns) == len(other.columns)
        for col in self.columns:
            other_column = other.get(col.name)
            is_equal = is_equal and other_column == col
        return is_equal
