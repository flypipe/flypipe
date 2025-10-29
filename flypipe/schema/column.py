from dataclasses import dataclass
from enum import Enum

from flypipe.config import get_config
from flypipe.schema.types import Type

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flypipe.node import Node


class RelationshipType(Enum):
    MANY_TO_MANY = "N:N"
    ONE_TO_ONE = "1:1"
    ONE_TO_MANY = "1:N"
    MANY_TO_ONE = "N:1"

    def __repr__(self):
        return self.name


@dataclass
class Relationship:
    type: RelationshipType
    description: str = ""

    def __repr__(self):
        return f"Relationship: {self.type}, {self.description}"


class Column:
    """Defines a column in the output of a flypipe node.

    Parameters:
        name (str): Name of the column.
        type (flypipe.schema.types.Type): Data type of the column.
        description (str,optional): A description of the column.
        pk (bool,optional): Marks the column as primary key or not. Defaults to `False`.
    """

    def __init__(
        self,
        name: str,
        type: Type,
        description: str = "",
        pk: bool = False,
    ):
        self.name = name
        self.type = type
        if not description and get_config("require_schema_description"):
            raise ValueError(
                f"Descriptions on schema columns configured as mandatory but no description provided for column "
                f"{self.name}"
            )
        self.description = description

        # Each column knows who is the node that it is associated with, it is used to map the Relationship between
        # node1.output_schema.col1 and node2.output_schema.col2. In this way, col1 knows that belongs to node1
        # and col2 to node2
        self.parent = None

        # this is a dict that holds the foreign keys between this column and other nodes columns,
        # the key is other Column, and the value holds Relationship
        self.relationships = {}

        self.pk = pk

    def __repr__(self):
        foreign_key = []
        for dest, relationship in self.relationships.items():
            description = (
                "" if not relationship.description else f" ({relationship.description})"
            )
            foreign_key.append(
                f"{self.parent.function.__name__}.{self.name} {relationship.type.value}{description} {dest.parent.function.__name__}.{dest.name}"
            )
        if foreign_key:
            foreign_key = "\n\t\t".join(foreign_key)
            foreign_key = f"\n\tForeign Keys:\n\t\t{foreign_key}"
        else:
            foreign_key = ""

        s = f"""
    Parent: {'None' if self.parent is None else self.parent.function.__name__}
    Column: {self.name}
    Data Type: {str(self.type)}
    Description: '{self.description}'{foreign_key}
    PK: {self.pk}
        """

        return s

    def _set_parent(self, parent: "Node"):
        self.parent = parent

    def _set_relationships(self, relationships: dict = None, reset: bool = False):
        relationships = relationships or {}
        self.relationships = {} if reset else relationships

    def reset_relationships(self):
        self._set_relationships(reset=True)

    def set_pk(self, pk: bool) -> "Column":
        self.pk = pk
        return self

    def reset_pk(self):
        self.set_pk(False)

    def copy(self):
        col = Column(self.name, self.type, description=self.description, pk=self.pk)
        col._set_parent(self.parent)
        col._set_relationships(self.relationships)
        return col

    @property
    def key(self):
        return (self.parent.key if self.parent is not None else "") + self.name

    def __hash__(self):
        # Needs a hash do differentiate between col2 of node1 and col2 of node 3
        return hash(self.key)

    def __eq__(self, other):
        is_equal = hash(self) == hash(other)
        is_equal = is_equal and self.parent == other.parent
        is_equal = is_equal and self.relationships == other.relationships
        is_equal = is_equal and type(self.type) is type(other.type)
        is_equal = is_equal and self.description == other.description
        is_equal = is_equal and self.pk == other.pk
        return is_equal

    def _add_relationship(
        self,
        other: "Column",
        relationship_type: RelationshipType,
        description: str = None,
    ):
        if self.get_foreign_key(other) is not None:
            raise ValueError(
                f"Multiple relationships have been set for the same column {self.name}"
            )
        self.relationships[other] = Relationship(relationship_type, description)
        return self

    def many_to_one(self, other: "Column", description: str = None):
        """Adds a N:1 relationship between this column and other node output column

        Args:
            other (Column): Node output column.
            description (str,optional): A description of the relationship between this column and other node output column.

        Usage:

        ``` py
        @node(
            ...
            output=Schema(
                ...
                Column("col_name", String(), "description)
                .many_to_one(another_node.output.col, "relationship description")
                ...
            )
        )
        def my_node(...):
            ...
        ```
        """
        return self._add_relationship(other, RelationshipType.MANY_TO_ONE, description)

    def one_to_many(self, other: "Column", description: str = None):
        """Adds a 1:N relationship between this column and other node output column

        Args:
            other (Column): Node output column.
            description (str,optional): A description of the relationship between this column and other node output column.

        Usage:

        ``` py
        @node(
            ...
            output=Schema(
                ...
                Column("col_name", String(), "description)
                .one_to_many(another_node.output.col, "relationship description")
                ...
            )
        )
        def my_node(...):
            ...
        ```
        """

        return self._add_relationship(other, RelationshipType.ONE_TO_MANY, description)

    def many_to_many(self, other: "Column", description: str = None):
        """Adds a N:N relationship between this column and other node output column

        Args:
            other (Column): Node output column.
            description (str,optional): A description of the relationship between this column and other node output column.

        Usage:

        ``` py
        @node(
            ...
            output=Schema(
                ...
                Column("col_name", String(), "description)
                .many_to_many(another_node.output.col, "relationship description")
                ...
            )
        )
        def my_node(...):
            ...
        ```
        """

        return self._add_relationship(other, RelationshipType.MANY_TO_MANY, description)

    def one_to_one(self, other: "Column", description: str = None):
        """Adds a 1:1 relationship between this column and other node output column

        Args:
            other (Column): Node output column.
            description (str,optional): A description of the relationship between this column and other node output column.

        Usage:

        ``` py
        @node(
            ...
            output=Schema(
                ...
                Column("col_name", String(), "description)
                .one_to_one(another_node.output.col, "relationship description")
                ...
            )
        )
        def my_node(...):
            ...
        ```
        """
        return self._add_relationship(other, RelationshipType.ONE_TO_ONE, description)

    def get_foreign_key(self, other: "Column"):
        for col, fk in self.relationships.items():
            if col == other:
                return fk
