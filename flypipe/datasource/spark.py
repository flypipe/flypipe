import re

from flypipe.node import node
from flypipe.node_type import NodeType

from flypipe.utils import get_logger

logger = get_logger()


def Spark(table):
    """Creates a flypipe node that loads a Spark table

    .. deprecated::
        This datasource is deprecated and will be removed in a future version.
        Please use alternative methods for loading Spark tables.

    Attributes:
        table (str): name of the spark table table

    Returns:
        Node

    Usage:
    ``` py
    @node(
        ...
        dependencies = [
            Spark("your_spark_table_name_here").select("column1", "column2",...).alias("any_alias_df")
        ]
        ...
    )
    def my_transformation(any_alias_df):
        return any_alias_df
    ```
    """

    @node(
        type="pyspark",
        description=f"Spark datasource on table {table}",
        tags=["datasource"],
        spark_context=True,
    )
    def spark_datasource(spark):
        logger.warning(
            "WARNING: Spark datasource is DEPRECATED and will be removed in a future version."
        )

        if spark is None:
            raise ValueError("Please provide a spark session, i.e. node.run(spark)")

        return spark.table(table)

    spark_datasource.function.__name__ = table

    key = f"spark.{table}"
    spark_datasource.key = re.sub(r"[^\da-zA-Z]", "_", key)
    spark_datasource.node_type = NodeType.DATASOURCE
    return spark_datasource
