from functools import partial

from flypipe.datasource.singleton import SingletonMeta
from flypipe.node import Node


# instances = {}
from flypipe.node_type import NodeType


class Spark(metaclass=SingletonMeta):
    """
    Abstract class to connect to Spark Datasource

    TODO: we need to make sure this is threadsafe, the current singleton implementation is not
    """

    def __init__(self, table):
        self.table = table
        self.all_selected_columns = []
        self.func = None

    def select(self, *columns):
        if isinstance(columns[0], list):
            columns = columns[0]
        else:
            columns = [column for column in columns]

        self.all_selected_columns = sorted(list(dict.fromkeys(self.all_selected_columns + columns)))
        func = partial(self.spark_datasource,
                       table=self.table,
                       columns=self.all_selected_columns)

        func.__name__ = self.table
        node = spark_datasource_node(type='pyspark',
                               description=f"Spark table {self.table}",
                               spark_context=True,
                               selected_columns = columns)

        func = node(func)
        func.grouped_selected_columns = self.all_selected_columns
        self.func = func
        return self.func

    @staticmethod
    def spark_datasource(spark, table, columns):
        assert spark is not None, 'Error: spark not provided'

        df = spark.table(table)
        if columns:
            return df.select(columns)

        return df

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