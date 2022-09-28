from typing import List

from flypipe.schema.column import Column


class Schema:

    def __init__(self, columns: List[Column]):
        self.columns = columns