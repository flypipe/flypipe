# import os
from pyspark.sql import SparkSession
from pythonping import ping


# dir_path = os.path.dirname(os.path.realpath(__file__))

def url_ok(url):
    try:
        ping("thrift://hive-metastore:9083", verbose=True, count=0)
        return True
    except Exception as e:
        return False


def get_spark():
    configs = (
        SparkSession
            .builder
            .config("spark.sql.warehouse.dir", "/spark-warehouse")
            .config("spark.sql.repl.eagerEval.enabled", "true")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .master("local[1]")
            .config("spark.submit.deployMode", "client")
            .appName("flypipe")
    )

    thrift_url = "thrift://hive-metastore:9083"
    thrift_server_available = url_ok(thrift_url)

    if thrift_server_available:
        configs = (
            configs
                .config("hive.metastore.uris", thrift_url)
                .enableHiveSupport()
        )

    return configs.getOrCreate()


spark = get_spark()

# spark = (
#     SparkSession
#         .builder
#         .config("spark.sql.warehouse.dir", "/spark-warehouse")
#         .config("hive.metastore.uris", "thrift://hive-metastore:9083")
#         .config("spark.sql.repl.eagerEval.enabled", "true")
#         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
#         .master("local[1]")
#         .config("spark.submit.deployMode", "client")
#         .appName("flypipe")
#         .enableHiveSupport()
#         .getOrCreate()
# )
#
#
#
# def drop_database(spark, db_name):
#     dir_path = os.path.dirname(os.path.realpath(__file__))
#     spark.sql(f"drop database if exists {db_name}")
#     path = os.path.join(dir_path,
#                         'spark-warehouse',
#                         f"{db_name}.db")
#     shutil.rmtree(path, ignore_errors=True)
