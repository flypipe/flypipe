from flypipe.schema.types import Type


class Column:

    def __init__(self, name: str, type: Type, description: str):
        self.name = name
        self.type = type
        self.description = description

    def __repr__(self):
        return f'Column("{self.name}", {str(self.type)}, "{self.description}")'