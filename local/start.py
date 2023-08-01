# noinspection PyUnresolvedReferences
import warnings

import IPython

import os

from pyspark.sql import SparkSession

# Avoid WARNING:root:'PYARROW_IGNORE_TIMEZONE' environment variable was not set
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"


def build_spark():
    configs = (
        SparkSession.builder.appName("flypipe")
        .master("local[1]")
        .config("spark.submit.deployMode", "client")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.liveUpdate.period", "-1")
        .config("spark.sql.repl.eagerEval.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")

        .enableHiveSupport()
        .config("hive.metastore.uris", "thrift://flypipe-hive-metastore:9083")
        .config("spark.sql.warehouse.dir", "/data/warehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.13:2.1.1,org.postgresql:postgresql:42.6.0",
        )
    )

    spark = configs.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

spark = build_spark()

warnings.filterwarnings("ignore")

ipython = IPython.get_ipython()
if "ipython" in globals():
    ipython.magic("load_ext autoreload")
    ipython.magic("autoreload 2")

html_width = "100%"
html_height = "1000"


def displayHTML(html):
    from html import escape

    html_to_display = f"""
    <iframe 
        srcdoc="{escape(html)}" 
        width="{html_width}" 
        height="{html_height}"
        scrolling="no"
        style="border: none; overflow:hidden; overflow-y:hidden;"
    ></iframe>
    """
    return IPython.display.HTML(html_to_display)


from IPython.core.magic import register_cell_magic
from IPython.core.display import HTML


@register_cell_magic
def sql(line, cell):
    display(HTML("<style>pre { white-space: pre !important; }</style>"))

    queries = cell.split(";")
    for query in queries:
        display(spark.sql(query).show(n=1000, truncate=False))
    return ""