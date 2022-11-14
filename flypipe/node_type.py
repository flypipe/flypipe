from enum import Enum


class NodeType(Enum):
    DATASOURCE = "DataSource"
    TRANSFORMATION = "Transformation"
    GENERATOR = "Generator"
