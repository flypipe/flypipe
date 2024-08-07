from pyspark.sql import SparkSession

from flypipe.cache import CacheMode
from flypipe.cache.cache import Cache


class CacheContext:
    def __init__(
        self,
        cache_mode: CacheMode = None,
        spark: SparkSession = None,
        cache: Cache = None,
    ):
        self.spark = spark
        self.cache = cache
        self.cache_mode = CacheMode.DISABLE if self.cache is None else cache_mode
        self._exists_cache_to_load = (
            None  # can not be True or False as both means exist or not exist
        )

    @property
    def disabled(self):
        return self.cache_mode == CacheMode.DISABLE

    @property
    def merge(self):
        return self.cache_mode == CacheMode.MERGE

    @property
    def exists_cache_to_load(self):
        if self.disabled:
            return False

        if self.merge:
            return False

        # this avoids calling self.exits() multiple times by NodeGraph._calculate_run_status method
        if self._exists_cache_to_load is not None:
            return self._exists_cache_to_load

        return self.exists()

    def read(self):
        if self.disabled:
            raise RuntimeError("Cache disabled, cannot read")

        if self.spark:
            return self.cache.read(self.spark)
        return self.cache.read()

    def write(self, df):
        if not self.disabled or self.merge:

            if self.spark:
                return self.cache.write(self.spark, df)
            return self.cache.write(df)

        return None

    def exists(self):
        if self.disabled:
            raise RuntimeError("Cache disabled, cannot check if exists")

        if self.spark:
            self._exists_cache_to_load = self.cache.exists(self.spark)
        else:
            self._exists_cache_to_load = self.cache.exists()
        return self._exists_cache_to_load
