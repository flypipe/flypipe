import os
from uuid import uuid4
from pyspark.sql import SparkSession

# Avoid WARNING:root:'PYARROW_IGNORE_TIMEZONE' environment variable was not set
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"


def build_spark():
    if os.environ.get("USE_SPARK_CONNECT") == "1":
        print("Building spark session (spark_connect)")

        return (
            SparkSession.builder.appName(str(uuid4()))
            .remote("sc://spark-connect:15002")
            .config("spark.sql.repl.eagerEval.enabled", "true")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate()
        )

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
