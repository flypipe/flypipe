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
    description: str = None

    def __repr__(self):
        return f"Relationship: {self.type}, {self.description}"


class Column:
    """
    Defines a column in the output of a flypipe node.

    Parameters
    ----------

    name : str
        Name of the column
    type : flypipe.schema.types.Type
        Data type of the column
    description : str, optional
        A description of the column
    kwargs : dict, optional
        Extra keyword arguments you want to document the column
    """

    def __init__(
        self,
        name: str,
        type: Type,
        description: str = "",
        **kwargs,
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
        # node1.output.col1 and node2.output.col2. In this way, col1 knows that belongs to node1
        # and col2 to node2
        self.parent = None

        # this is a dict that holds the foreign keys between this column and other nodes columns,
        # the key is other Column, and the value holds Relationship
        self.foreign_keys = {}

        # The user can define other named arguments need for their one documentation, like primary keys.
        self.kwargs = kwargs
        if self.kwargs is not None:
            for key, value in self.kwargs.items():
                setattr(self, key, value)

    def __repr__(self):
        foreign_key = []
        for dest, relationship in self.foreign_keys.items():
            foreign_key.append(
                f"{self.parent.function.__name__}.{self.name} {relationship.type.value} {dest.parent.function.__name__}.{dest.name}"
            )
        if foreign_key:
            foreign_key = "\n\t\t".join(foreign_key)
            foreign_key = f"\n\tForeign Keys:\n\t\t{foreign_key}"
        else:
            foreign_key = ""

        s = f"""
    Column: {self.name}
    Parent: {self.parent.function.__name__}
    Data Type: {str(self.type)}
    Description: {self.description}{foreign_key}
        """

        return s

    def _set_parent(self, parent: "Node"):
        self.parent = parent

    def _set_foreign_keys(self, foreign_keys: dict):
        self.foreign_keys = foreign_keys

    def copy(self):
        col = Column(self.name, self.type, self.description, **self.kwargs)
        col._set_parent(self.parent)
        col._set_foreign_keys(self.foreign_keys)
        return col

    def __hash__(self):
        # Needs a hash do differentiate between col2 of node1 and col2 of node 3
        return hash((self.parent.key if self.parent is not None else "") + self.name)

    def __eq__(self, other):
        is_equal = hash(self) == hash(other)
        is_equal = is_equal and self.kwargs == other.kwargs
        is_equal = is_equal and self.foreign_keys == other.foreign_keys
        is_equal = is_equal and type(self.type) is type(other.type)
        is_equal = is_equal and self.description == other.description
        return is_equal

    def many_to_one(self, other: "Column", description: str = ""):
        """
        Adds a N:1 relationship between this column and other node output column

        Usage:

        .. highlight:: python
        .. code-block:: python

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
        """
        self.foreign_keys[other.copy()] = Relationship(
            RelationshipType.MANY_TO_ONE, description
        )
        return self

    def one_to_many(self, other: "Column", description: str = ""):
        """
        Adds a 1:N relationship between this column and other node output column

        Usage:

        .. highlight:: python
        .. code-block:: python

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
        """
        self.foreign_keys[other.copy()] = Relationship(
            RelationshipType.ONE_TO_MANY, description
        )
        return self

    def many_to_many(self, other: "Column", description: str = ""):
        """
        Adds a N:N relationship between this column and other node output column

        Usage:

        .. highlight:: python
        .. code-block:: python

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
        """
        if self.get_foreign_key(other) is not None:
            raise ValueError(
                f"Multiple relationships have been set for the same column {self.name}"
            )

        self.foreign_keys[other.copy()] = Relationship(
            RelationshipType.MANY_TO_MANY, description
        )
        return self

    def one_to_one(self, other: "Column", description: str = ""):
        """
        Adds a 1:1 relationship between this column and other node output column

        Usage:

        .. highlight:: python
        .. code-block:: python

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
        """
        self.foreign_keys[other.copy()] = Relationship(
            RelationshipType.ONE_TO_ONE, description
        )
        return self

    def get_foreign_key(self, other: "Column"):
        for col, fk in self.foreign_keys.items():
            if col == other:
                return fk
