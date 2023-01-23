from flypipe.config import get_config
from flypipe.schema.types import Type


class Column:  # pylint: disable=too-few-public-methods
    """
    Model for a column in the output of a flypipe transformation.
    """

    def __init__(
        self, name: str, type: Type, description: str = ""
    ):  # pylint: disable=redefined-builtin
        self.name = name
        self.type = type
        if not description and get_config("require_schema_description"):
            raise ValueError(
                f"Descriptions on schema columns configured as mandatory but no description provided for column "
                f"{self.name}"
            )
        self.description = description

    def __repr__(self):
        return f'Column("{self.name}", {str(self.type)}, "{self.description}")'
