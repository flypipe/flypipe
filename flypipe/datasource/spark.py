from functools import partial
from typing import List
from flypipe.datasource.datasource import DataSource
import pyspark.sql.functions as F

from flypipe.exceptions import ErrorTimeTravel
from flypipe.node import node


instances = {}


class Spark(DataSource):
    """
    Abstract class to connect to Spark Datasource

    TODO: we need to make sure this is threadsafe, the current singleton implementation is not
    """

    def __init__(self, table):
        self.table = table
        self.columns = set()
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
            self.columns = self.columns.union(set(columns[0]))
        else:
            for column in columns:
                self.columns.add(column)

        func = partial(self.spark_datasource, table=self.table, columns=list(self.columns))
        func.__name__ = self.table
        func = node(type='pyspark', spark_context=True, dependencies=[])(func)
        self.func = func
        return self.func

    @staticmethod
    def spark_datasource(spark, table, columns):
        df = spark.table(table)
        if columns:
            return df.select(columns)

        return df

