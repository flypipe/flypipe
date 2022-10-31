import re
from flypipe.node import node

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
    spark_datasource.key = re.sub('[^\da-zA-Z]', '_', key)

    return spark_datasource
