from flypipe.cache.cache_context import CacheMode


class Cache:
    """
    Base definition for a Node cache. The architecture supports customisation primarily via custom read/write/exist
    functions being passed to the constructor, the goal being to use a function approach rather than class inheritance
    to make it more beginner friendly.
    """

    def __init__(self, read, write, exists, *args, **kwargs):
        self._read = read
        self._write = write
        self._exists = exists
        self.args = args
        self.kwargs = kwargs

    def read(self, mode):
        if mode == CacheMode.DISABLE:
            raise Exception("Illegal operation- cache disabled")
        return self._read(*self.args, **self.kwargs)

    def write(self, mode, result):
        if mode == CacheMode.DISABLE:
            raise Exception("Illegal operation- cache disabled")
        self._write(result, *self.args, **self.kwargs)

    def exists(self, mode):
        if mode in (CacheMode.DISABLE, CacheMode.MERGE):
            return False
        return self._exists(*self.args, **self.kwargs)
