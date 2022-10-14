from functools import partial

from flypipe.datasource.singleton import SingletonMeta
from flypipe.node import datasource_node


# instances = {}


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
        node = datasource_node(type='pyspark',
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
