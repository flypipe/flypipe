"""TODO- deprecate or remove this"""

from pyspark.sql import SparkSession

_spark_session = None


def register_spark_session(spark):
    global _spark_session
    _spark_session = spark


def get_spark_session():
    global _spark_session
    if 'spark' in globals():
        spark = globals()['spark']
    elif _spark_session:
        spark = _spark_session
    else:
        # TODO- throw an exception rather than creating for the user, we are worried they will forget to declare a spark object and dependingo n the circumstance would end up with a suboptimal spark setup
        spark = (
            SparkSession
                .builder
                .master("local")
                .appName("flypipe")
                .getOrCreate()
        )
        _spark_session = spark
    return spark
