class OutputColumnSet:
    """
    Model for the set of columns that a node should output. When a dependent node requests columns from a node we
    invoke add_columns. For example suppose:
    - we have node t1:
    - node t2 is dependent on t1 and selects column c1. The output column set will be c1 after this is computed.
    - node t3 is dependent on t1 and selects columns c2, c3. The output column set will be c1, c2, c3 after this is
    computed.
    - node t4 is dependent on t1 and has no selected columns (which means it selects all columns). The output column
    set will be None (special value for all columns) after this is computed.
    """

    def __init__(self, columns):
        self.columns = set()
        self.add_columns(columns)

    def add_columns(self, new_columns):
        if new_columns is None or self.columns is None:
            self.columns = None
        else:
            for column in new_columns:
                self.columns.add(column)

    def get_columns(self):
        if self.columns is None:
            return None
        return sorted(list(self.columns))
