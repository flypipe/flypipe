from flypipe.schema.column import Column


class Schema:
    def __init__(self, *columns):
        self.columns = columns[0] if isinstance(columns[0], list) else list(columns)

    def get(self, column):
        for col in self.columns:
            if col.name == column:
                return col

    def get_column_names(self):
        return [column.name for column in self.columns]

    def __repr__(self):
        if not self.columns:
            return "Schema: no columns"

        cols = [str(col) for col in self.columns]
        return "Schema([\n\t" + ",\n\t".join(cols) + "\n])"

    def copy(self):
        return Schema(
            [
                Column(column.name, column.type, column.description)
                for column in self.columns
            ]
        )
