from flypipe.node import Node
from flypipe.spark.sql_node import SparkSQLNode


def register_types():
    Node.register_type('pandas', Node)
    try:
        import pyspark
    except ModuleNotFoundError:
        pass
    else:
        from flypipe.spark.node import SparkNode
        Node.register_type('spark', SparkNode)
        Node.register_type('spark_sql', SparkSQLNode)
