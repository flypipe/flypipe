from enum import Enum


class CacheMode(Enum):
    """
    Mode the cache is operating in.
    """
    NOT_EXISTS = -1
    DEFAULT = 0
    DISABLE = 1  # ignore any current cache hits, compute the underlying function and override the cache with the result
    MERGE = 2  # use for appending data or merge operations
