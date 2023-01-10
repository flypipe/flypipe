import os

from pyspark.sql import SparkSession
from pythonping import ping

# Avoid WARNING:root:'PYARROW_IGNORE_TIMEZONE' environment variable was not set
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"


def url_ok(url):
    try:
        ping("thrift://hive-metastore:9083", verbose=True, count=0)
        return True
    except Exception as e:
        return False


def get_spark():
    configs = (
        SparkSession.builder.config("spark.sql.warehouse.dir", "/spark-warehouse")
        .config("spark.sql.repl.eagerEval.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.liveUpdate.period", "-1")
        .master("local[1]")
        .config("spark.submit.deployMode", "client")
        .appName("flypipe")
    )

    thrift_url = "thrift://hive-metastore:9083"
    thrift_server_available = url_ok(thrift_url)

    if thrift_server_available:
        configs = (
            configs.config("hive.metastore.uris", thrift_url)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.jars.repositories", "https://mvnrepository.com")
            .config("spark.jars.packages", "io.delta:delta-core_2.13:2.1.1")
            .enableHiveSupport()
        )

    spark = configs.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


spark = get_spark()
