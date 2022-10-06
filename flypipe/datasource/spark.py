from functools import partial
from typing import List
from flypipe.datasource.datasource import DataSource
import pyspark.sql.functions as F

from flypipe.exceptions import ErrorTimeTravel
from flypipe.node import node, datasource_node
from flypipe.node_graph import NodeGraph

instances = {}


class Spark(DataSource):
    """
    Abstract class to connect to Spark Datasource

    TODO: we need to make sure this is threadsafe, the current singleton implementation is not
    """

    def __init__(self, table):
        self.table = table
        self.columns = []
        self.func = None

    @classmethod
    def table(cls, table):
        global instances
        if table not in instances:
            instances[table] = Spark(table)
        return instances[table]

    @classmethod
    def get_instance(cls, table):
        global instances
        return instances[table]

    def select(self, *columns):
        if isinstance(columns[0], list):
            self.columns = list(dict.fromkeys(self.columns + columns[0]))
        else:
            for column in columns:
                self.columns.append(column)

        func = partial(self.spark_datasource, table=self.table, columns=self.columns)
        func.__name__ = self.table
        node = datasource_node(type='pyspark',
                               description=f"Spark table {self.table}",
                               spark_context=True,
                               selected_columns = self.columns)

        func = node(func)
        self.func = func
        return self.func

    @staticmethod
    def spark_datasource(spark, table, columns):
        df = spark.table(table)
        if columns:
            return df.select(columns)

        return df

