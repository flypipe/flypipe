from flypipe.cache.cache import Cache


class DummyCache(Cache):
    """
    Cache that does nothing but implements the structure of a cache, we can give this as the cache to nodes with no
    specified cache, this avoids us having to constantly check if the node has a cache in code which ugly and tedious.
    """
    def __init__(self):
        pass

    def read(self, mode):
        return None

    def exists(self, mode):
        return False

    def write(self, mode, result):
        return None
