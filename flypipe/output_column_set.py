from enum import Enum

from flypipe.schema import Schema, Column
from flypipe.schema.types import Unknown


class OutputColumnStatus(Enum):
    ALL_COLUMNS = 0
    SOME_COLUMNS = 1
    UNKNOWN_COLUMNS = 2


class OutputColumnSet:
    """
    This class tracks the columns we're expected to output from a node. This is quite complicated as there are a number
    of factors:
    - If a node is used by descendant nodes with specific columns, then we want to select only those columns.
    - If a node has an output schema then the output schema ought to be a hard limit on the output columns. I.e if the
    output schema is columns c1, c2, c3 then it doesn't matter if c4 is requested, we should limit and expect descendant
    nodes to be able to access c1, c2 & c3.
    - If a descendant node doesn't request any particular column then we should pull all available columns from the
    parent.
    """

    def __init__(self, schema=None):
        self._schema = schema
        if schema:
            self._schema_columns = set([column.name for column in schema.columns])
            self._status = OutputColumnStatus.SOME_COLUMNS
        else:
            self._schema_columns = None
            self._status = OutputColumnStatus.UNKNOWN_COLUMNS
        self._columns = set()

    def add_columns(self, columns):
        # All columns already selected, do nothing
        if self._status == OutputColumnStatus.ALL_COLUMNS:
            return
        # Select all available columns
        if columns is None:
            if not self._schema_columns:
                self._columns = set()
            else:
                self._columns = set(self._schema_columns)
            self._status = OutputColumnStatus.ALL_COLUMNS
        else:
            self._status = OutputColumnStatus.SOME_COLUMNS
            self._columns = self._columns.union(set(columns))
            if self._schema_columns:
                self._columns = self._columns.intersection(self._schema_columns)

    @property
    def columns(self):
        return sorted(list(self._columns))

    @property
    def status(self):
        return self._status

    @property
    def schema(self):
        if self.status == OutputColumnStatus.UNKNOWN_COLUMNS or \
                (self.status == OutputColumnStatus.ALL_COLUMNS and not self.columns):
            return None
        output_schema_columns = []
        if self._schema:
            for column in self._schema.columns:
                if column.name in self.columns:
                    output_schema_columns.append(column)
        else:
            for column_name in self.columns:
                output_schema_columns.append(Column(column_name, Unknown()))
        return Schema(output_schema_columns)
