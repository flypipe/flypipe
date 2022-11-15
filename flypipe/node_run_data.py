from enum import Enum

from flypipe.schema import Schema, Column
from flypipe.schema.types import Unknown


ALL_OUTPUT_COLUMNS = -1


class NodeRunData:
    """
    When we run a node, we generate some auxiliary node information that is relevant only for that run. We store such
    information in this model rather than the Node model which holds the static definition model for the Node.
    """

    def __init__(self, output_schema=None):
        self._output_schema = output_schema
        self._output_columns = set()

    def add_output_columns(self, columns):
        if columns is None:
            self._add_all_output_columns()
        else:
            if self._output_columns != ALL_OUTPUT_COLUMNS:
                if self._output_schema:
                    output_schema_columns = set(self._output_schema.get_column_names())
                    if set(columns) - output_schema_columns:
                        raise ValueError(
                            f'Requested columns {set(columns) - output_schema_columns} that are not present on the '
                            f'node')
                self._output_columns = self._output_columns.union(set(columns))

    def _add_all_output_columns(self):
        if self._output_schema:
            self._output_columns = set(self._output_schema.get_column_names())
        else:
            self._output_columns = ALL_OUTPUT_COLUMNS

    @property
    def output_columns(self):
        if self._output_columns == ALL_OUTPUT_COLUMNS:
            return None
        else:
            return sorted(list(self._output_columns))

    @property
    def output_schema(self):
        output_schema_columns = []
        columns = self.output_columns
        if self._output_schema:
            for column in self._output_schema.columns:
                if column.name in columns:
                    output_schema_columns.append(column)
        else:
            # Unknown columns & schema
            if columns is None:
                return None
            for column in columns:
                output_schema_columns.append(Column(column, Unknown()))
        return Schema(output_schema_columns)
