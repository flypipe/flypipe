from enum import Enum


class NodeType(Enum):
    """
    Hold the list of possible types each Node can fall under
    """

    DATASOURCE = "Datasource"
    TRANSFORMATION = "Transformation"
    NODE_FUNCTION = "Node Function"
