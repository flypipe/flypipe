from flypipe.cache import CacheOperation


class CacheContext:

    def __init__(self, cache_operation=None, spark=None, cache=None):
        self.cache_operation = cache_operation or {}
        self.spark = spark
        self.cache = cache

    def create(self, node):

        if node.cache is None:
            return None

        cache_operation = (
            self.cache_operation[node] if node in self.cache_operation else None
        )
        cache_context = CacheContext(cache_operation, self.spark, node.cache)

        if cache_context.disabled:
            return None

        return cache_context

    @property
    def disabled(self):
        return self.cache_operation == CacheOperation.DISABLE

    @property
    def merge(self):
        return self.cache_operation == CacheOperation.MERGE

    def read(self):
        if self.disabled:
            raise RuntimeError("Cache disabled, cannot read")
        return self.cache.read(self.spark)

    def write(self, df):
        if self.disabled:
            raise RuntimeError("Cache disabled, cannot write")
        return self.cache.write(self.spark, df)

    def exists(self):
        if self.disabled:
            raise RuntimeError("Cache disabled, cannot check if exists")
        return self.cache.exists(self.spark)
