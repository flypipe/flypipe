class OutputColumnSet:

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
