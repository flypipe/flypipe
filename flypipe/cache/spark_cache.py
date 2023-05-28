from flypipe.cache.cache import Cache
from flypipe.cache.cache_context import CacheMode


class SparkCache(Cache):

    def read(self, mode, spark):
        if mode == CacheMode.DISABLE:
            raise Exception("Illegal operation- cache disabled")
        return self._read(spark, *self.args, **self.kwargs)

    def write(self, mode, spark, result):
        if mode == CacheMode.DISABLE:
            raise Exception("Illegal operation- cache disabled")
        self._write(spark, result, *self.args, **self.kwargs)

    def exists(self, mode, spark):
        if mode in (CacheMode.DISABLE, CacheMode.MERGE):
            return False
        return self._exists(spark, *self.args, **self.kwargs)
