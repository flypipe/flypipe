from functools import partial

from flypipe.node import Node
from flypipe.node_type import NodeType


class Spark:
    """
    Abstract class to connect to Spark Datasource
    """

    def __init__(self, table):
        self.table = table
        self.selected_columns = []
        self.func = None

    def select(self, *columns):

        if isinstance(columns[0], list):
            self.selected_columns = list(dict.fromkeys(self.selected_columns + columns[0]))
        else:
            for column in columns:
                self.selected_columns.append(column)
        self.selected_columns = sorted(list(set(self.selected_columns)))

        func = partial(self.spark_datasource,
                       table=self.table)

        func.__name__ = self.table
        node = spark_datasource_node(type='pyspark',
                               description=f"Spark table {self.table}",
                               spark_context=True,
                               selected_columns = self.selected_columns)

        func = node(func)
        self.func = func
        return self.func


    @staticmethod
    def spark_datasource(spark, table):
        assert spark is not None, 'Error: spark not provided'

        return spark.table(table)


class SparkDataSource(Node):
    node_type = NodeType.DATASOURCE


def spark_datasource_node(*args, **kwargs):
    """
    Decorator factory that returns the given function wrapped inside a Datasource Node class
    """

    def decorator(func):
        """TODO: I had to re-create graph in the decorator as selected_columns are set after the node has been created
        when creting a virtual datasource node, it is set the columns manually
        """

        kwargs_init = {k:v for k,v in kwargs.items() if k != 'selected_columns'}
        ds = SparkDataSource(func, *args, **kwargs_init)
        return ds.select(kwargs['selected_columns'])

    return decorator