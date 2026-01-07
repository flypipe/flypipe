import os
from uuid import uuid4
from pyspark.sql import SparkSession

# Avoid WARNING:root:'PYARROW_IGNORE_TIMEZONE' environment variable was not set
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"


def build_spark(use_spark_connect: bool = False):
    if os.environ.get("USE_SPARK_CONNECT") == "1" or use_spark_connect:
        print("Building spark session (spark_connect)")

        spark = (
            SparkSession.builder.appName(str(uuid4()))
            .remote("sc://spark-connect:15002")
            # Fail fast if gRPC isn’t reachable (e.g., 20s)
            .config("spark.connect.grpc.client.deadlineSeconds", "20")
            # Log client connection details to stdout
            .config("spark.connect.client.verbose", "true")
            # your prefs
            .config("spark.sql.repl.eagerEval.enabled", "true")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate()
        )
        return spark

    print("Building spark session")

    spark = (
        SparkSession.builder.appName(str(uuid4()))
        .master("local[1]")
        .config("spark.driver.host", "localhost")
        .config("spark.submit.deployMode", "client")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.liveUpdate.period", "-1")
        .config("spark.sql.repl.eagerEval.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark
