from enum import Enum


class Mode(Enum):
    PANDAS = "pandas"
    PANDAS_ON_SPARK = "pandas_on_spark"
    PYSPARK = "pyspark"
    SPARK_SQL = "spark_sql"
