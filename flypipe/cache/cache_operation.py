from enum import Enum


class CacheOperation(Enum):
    DISABLE = 0
    MERGE = 1  # will call write function independent cache exists or not, use for appending data or merge operations
