from enum import Enum


class CacheOperation(Enum):
    # disable the cache for read/write i.e the underlying transformation will always be run + the cache won't be
    # filled afterwards
    DISABLE = 0
    # ignore any current cache hits, compute the underlying function and override the cache with the result
    # use for appending data or merge operations
    MERGE = 1
