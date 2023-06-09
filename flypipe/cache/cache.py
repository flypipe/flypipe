from abc import ABC, abstractmethod


# pylint: disable=too-few-public-methods
class Cache(ABC):
    """
    Superclass for Caches
    """

    @abstractmethod
    def read(self, *args, **kwargs):
        raise RuntimeError("Please provide your own cache")

    @abstractmethod
    def write(self, *args, **kwargs):
        raise RuntimeError("Please provide your own cache")

    @abstractmethod
    def exists(self, *args, **kwargs):
        raise RuntimeError("Please provide your own cache")
