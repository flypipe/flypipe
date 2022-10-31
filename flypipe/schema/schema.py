from typing import List

from flypipe.schema.column import Column


class Schema:

    def __init__(self, columns: List[Column]):
        self.columns = columns

    def apply(self, df_wrapper):
        pass

    def __repr__(self):
        if not self.columns:
            return "Schema: no columns"

        cols = [str(col) for col in self.columns]
        return "Schema([\n\t" + ",\n\t".join(cols) + "\n])"