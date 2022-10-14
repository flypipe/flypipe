from abc import ABC


class DataSource(ABC):
    """
    Automatically builds a transformation that pulls data from an external source such as a database or a csv file.
    Responsibilities:
    - Select only columns which are specified by successor nodes.
    - Union all the dependant columns for the query from the datasource, such that we only need to run the datasource
    query once for all children. For example:
                 T1
               / col1
    Datasource
               \ col2
                 T2
    The datasource will do one query select col1, col2 from datasource for both successor nodes.
    - Quietly create a graph node for each different datasource defined in the node dependencies, such that if we print
    the graph for a transformation the datasources show up.
    """
    pass