import re
from flypipe.node import node
from flypipe.node_type import NodeType


def Spark(table):
    @node(
        type='pyspark',
        description=f'Spark datasource on table {table}',
        tags=['datasource'],
        spark_context=True,
    )
    def spark_datasource(spark):
        return spark.table(table)

    spark_datasource.function.__name__ = table

    key = f"spark.{table}"
    spark_datasource.key = re.sub('[^\da-zA-Z]', '-', key)
    spark_datasource.node_type = NodeType.DATASOURCE
    return spark_datasource
