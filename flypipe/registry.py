from flypipe.node import Node
from flypipe.pandas.node import PandasNode
from flypipe.spark.sql_node import SparkSQLNode


def register_types():
    Node.register_node_type(PandasNode)
    try:
        import pyspark
    except ModuleNotFoundError:
        pass
    else:
        from flypipe.spark.node import SparkNode
        Node.register_node_type(SparkNode)
        # Node.register_node_type('spark_sql', SparkSQLNode)
