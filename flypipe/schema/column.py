from dataclasses import dataclass
from enum import Enum

from flypipe.config import get_config
from flypipe.schema.types import Type

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flypipe.node import Node

class RelationshipType(Enum):
    MANY_TO_MANY="N:N"
    ONE_TO_ONE="1:1"
    ONE_TO_MANY="1:N"
    MANY_TO_ONE="N:1"

    def __repr__(self):
        return self.name

@dataclass
class Relationship:
    relationship_type: RelationshipType
    description: str = None

    def __repr__(self):
        return f"{self.relationship_type.value}: {self.description}"

class Column:
    """
    Model for a column in the output of a flypipe transformation.
    """

    def __init__(self, name: str, type: Type, description: str = "", parent: "Node" = None, **kwargs):
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
        self.parent = parent

        # this is a dict that holds the foreign keys between this column and other nodes columns,
        # the key is other Column, and the value holds Relationship
        self.fks = {}

        # The user can define other named arguments need for their one documentation, like primary keys.
        self.kwargs = kwargs
        if self.kwargs is not None:
            for key, value in self.kwargs.items():
                setattr(self, key, value)

    def __repr__(self):

        if self.parent is not None:
            return f'Node:{self.parent.key}.{self.name} FKS={self.fks})'
        else:
            return f'Node:ORPHAN_COLUMN.{self.name} FKS={self.fks})'

    def set_parent(self, parent: "Node"):
        self.parent = parent

    def copy(self):
        return Column(self.name, self.type, self.description, parent=self.parent, **self.kwargs)

    def __hash__(self):
        # Needs a hash do differentiate between col2 of node1 and col2 of node 3
        return hash(self.parent.key + self.name)

    def many_to_one(self, other: "Column", description: str = ""):
        self.fks[other] = Relationship(RelationshipType.MANY_TO_ONE, description)
        return self

    def one_to_many(self, other: "Column", description: str = ""):
        self.fks[other] = Relationship(RelationshipType.ONE_TO_MANY, description)
        return self

    def many_to_many(self, other: "Column", description: str = ""):
        self.fks[other] = Relationship(RelationshipType.MANY_TO_MANY, description)
        return self

    def one_to_one(self, other: "Column", description: str = ""):
        self.fks[other] = Relationship(RelationshipType.ONE_TO_ONE, description)
        return self


