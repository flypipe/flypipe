from abc import ABC, abstractmethod
from flypipe.cache import Cache


class CDCCache(Cache, ABC):
    """
    Superclass for CDCCaches
    """

    @abstractmethod
    def read_cdc(self, *args, **kwargs):
        raise RuntimeError("Please provide your own cache")

    @abstractmethod
    def write_cdc(self, *args, **kwargs):
        raise RuntimeError("Please provide your own cache")
