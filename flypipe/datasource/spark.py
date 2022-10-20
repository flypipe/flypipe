from functools import partial

from flypipe.node import Node
from flypipe.node_type import NodeType


class Spark:
    """
    Abstract class to connect to Spark Datasource
    """

    def __init__(self, table):
        func = partial(self.spark_datasource, table=table)
        func.__name__ = table
        self.node = spark_datasource_node(type='pyspark',
                                          description=f"Spark table {table}",
                                          spark_context=True)

        func = self.node(func)
        self.func = func


    def select(self, *columns):

        if isinstance(columns[0], list):
            self.func.selected_columns = list(dict.fromkeys(self.func.selected_columns + columns[0]))
        else:
            for column in columns:
                self.func.selected_columns.append(column)
        self.func.selected_columns = sorted(list(set(self.func.selected_columns)))
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
        return SparkDataSource(func, *args, **kwargs)

    return decorator