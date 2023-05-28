from flypipe.cache.cache_context import CacheContext, CacheMode


class Cache:
    """
    Base definition for a Node cache. The architecture supports customisation primarily via custom read/write/exist
    functions being passed to the constructor, the goal being to use a function approach rather than class inheritance
    to make it more beginner friendly.
    """

    def __init__(self, read, write, exists):
        self._read = read
        self._write = write
        self._exists = exists

    def read(self, cache_context: CacheContext):
        if cache_context.mode == CacheMode.DISABLE:
            raise Exception("Illegal operation- cache disabled")
        return self._read(cache_context)

    def write(self, cache_context: CacheContext):
        if cache_context.mode == CacheMode.DISABLE:
            raise Exception("Illegal operation- cache disabled")
        self._write(cache_context)

    def exists(self, cache_context: CacheContext):
        if cache_context.mode in (CacheMode.DISABLE, CacheMode.MERGE):
            return False
        return self._exists(cache_context)
