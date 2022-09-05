from pyspark.sql import SparkSession

spark = (
    SparkSession
        .builder
        .config("spark.sql.warehouse.dir", "/spark-warehouse")
        .config("hive.metastore.uris", "thrift://hive-metastore:9083")
        .config("spark.sql.repl.eagerEval.enabled", True)
        .master("local")
        .appName("flypipe")
        .enableHiveSupport()
        .getOrCreate()
)
