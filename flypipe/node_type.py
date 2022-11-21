from enum import Enum


class NodeType(Enum):
    DATASOURCE = "Datasource"
    TRANSFORMATION = "Transformation"
    NODE_FUNCTION = "Node Function"
