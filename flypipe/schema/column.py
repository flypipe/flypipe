class Column:

    def __init__(self, name, type, description=None):
        self.name = name
        self.type = type
        self.description = description or "No description"