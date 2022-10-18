import shutil
import os
from pyspark.sql import SparkSession
dir_path = os.path.dirname(os.path.realpath(__file__))

spark = (
    SparkSession
    .builder
    .config("spark.sql.warehouse.dir", os.path.join(dir_path, "spark-warehouse"))
    .master("local[1]")
    .config("spark.submit.deployMode", "client")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .getOrCreate()
)


def drop_database(spark, db_name):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    spark.sql(f"drop database if exists {db_name}")
    path = os.path.join(dir_path,
                        'spark-warehouse',
                        f"{db_name}.db")
    shutil.rmtree(path)