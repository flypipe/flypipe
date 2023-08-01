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
    )

    spark = configs.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


spark = build_spark()
