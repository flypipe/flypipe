from enum import Enum


class CacheMode(Enum):
    """
    Mode the cache is operating in.
    """

    DEFAULT = 0
    # disable the cache for read/write i.e the underlying transformation will always be run + the cache won't be
    # filled afterwards
    DISABLE = 1
    # ignore any current cache hits, compute the underlying function and override the cache with the result
    # use for appending data or merge operations
    MERGE = 2


class CacheContext:  # pylint: disable=too-few-public-methods
    """
    Stores any runtime-specific information for a cache to use, this gets passed into all cache operations.
    """

    def __init__(self, mode: CacheMode = CacheMode.DEFAULT):
        self.mode = mode
