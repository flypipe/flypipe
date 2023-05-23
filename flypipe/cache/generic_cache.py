from flypipe.cache.cache import Cache


class GenericCache(Cache):
    def __init__(self, read, write, exists, spark_context=False):
        self._read = read
        self._write = write
        self._exists = exists
        self.spark_context = spark_context

    def read(self, spark):
        if self.spark_context:
            return self._read(spark)
        return self._read()

    def exists(self, spark):
        if self.spark_context:
            return self._exists(spark)
        return self._exists()

    def write(self, spark, df):
        if self.spark_context:
            return self._write(spark, df)
        return self._write(df)
