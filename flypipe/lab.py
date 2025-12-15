"""
Lab file demonstrating static node behavior in a diamond graph.

Graph structure:
    D (cached source)
   / \
  /   \
 B     C
 |     |
(static) (normal CDC)
  \   /
   \ /
    A (final node)

- D is cached and produces data
- B depends on D.static() - will load ALL cached data from D (no CDC filtering)
- C depends on D normally - will apply CDC filtering based on timestamps
- A combines results from both B and C
"""

from flypipe import node
from flypipe.tests.cdc_manager_cache import CDCManagerCache
import pyspark.sql.functions as F


# Create cache for node D
cache_d = CDCManagerCache(
    schema="demo_schema",
    table="node_d",
    cdc_table="cdc_demo",
    merge_keys=["id"],
)


@node(type="pyspark", cache=cache_d)
def D():
    """Source node with cache - generates initial data"""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    data = [(1, "Product A"), (2, "Product B"), (3, "Product C")]
    return spark.createDataFrame(data, ["id", "name"]).withColumn(
        "cdc_datetime_updated", F.current_timestamp()
    )


@node(type="pyspark", dependencies=[D.static()])
def B(D):
    """
    Transformation that depends on D as STATIC.
    This means B will always load the COMPLETE cached dataset from D,
    regardless of CDC timestamps.
    """
    return D.withColumn("processed_by", F.lit("B")).withColumn(
        "b_value", F.col("id") * 10
    )


@node(type="pyspark", dependencies=[D])
def C(D):
    """
    Transformation that depends on D normally.
    This means C will apply CDC filtering - only loading new/changed data from D.
    """
    return D.withColumn("processed_by", F.lit("C")).withColumn(
        "c_value", F.col("id") * 100
    )


@node(type="pyspark", dependencies=[B, C])
def A(B, C):
    """
    Final node that combines results from B and C.
    B will have all data from D (static),
    C will have only new/changed data from D (CDC filtered).
    """
    # Add a marker column to each dataframe before union
    b_marked = B.select(
        F.col("id").alias("b_id"),
        F.col("name").alias("b_name"),
        F.col("b_value"),
        F.lit("from_B").alias("source"),
    )

    c_marked = C.select(
        F.col("id").alias("c_id"),
        F.col("name").alias("c_name"),
        F.col("c_value"),
        F.lit("from_C").alias("source"),
    )

    # Combine results
    return b_marked.unionByName(
        c_marked,
        allowMissingColumns=True,
    )


if __name__ == "__main__":
    # Generate graph visualization
    from flypipe.tests.spark import build_spark
    spark = build_spark()
    print("📊 Generating graph visualization...")

    html = A.html(spark)

    with open("./graph2.html", "w") as f:
        f.write(html)

    print("✅ Graph visualization saved to graph.html")
    print("   Open graph.html in your browser to view the pipeline structure")

