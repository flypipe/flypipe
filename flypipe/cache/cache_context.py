from pyspark.sql import SparkSession

from flypipe.cache import CacheMode
from flypipe.cache.cache import Cache


class CacheContext:

    def __init__(self, cache_mode: CacheMode=None, spark: SparkSession=None, cache: Cache=None):
        self.spark = spark
        self.cache = cache
        self.cache_mode = cache_mode or CacheMode.DEFAULT
        self.cache_mode = self.cache_mode if cache else None
        self._exists_cache_to_load = None

    @property
    def runtime_load(self):
        if self.disabled:
            return False

        if not self.cache:
            return False

        if self.merge:
            return False

        if self.exists_cache_to_load is not None:
            return self.exists_cache_to_load

        return self.exists()

    @property
    def default(self):
        return self.cache_mode == CacheMode.DEFAULT
    @property
    def disabled(self):
        return self.cache_mode == CacheMode.DISABLE

    @property
    def merge(self):
        return self.cache_mode == CacheMode.MERGE

    def read(self):
        if self.disabled:
            raise RuntimeError("Cache disabled, cannot read")
        return self.cache.read(self.spark)

    def write(self, df):
        if not self.disabled and (self.default or self.merge):
            return self.cache.write(self.spark, df)

    @property
    def exists_cache_to_load(self):
        return self._exists_cache_to_load

    def exists(self):
        if self.disabled:
            raise RuntimeError("Cache disabled, cannot check if exists")

        self._exists_cache_to_load = self.cache.exists(self.spark)
        return self._exists_cache_to_load
