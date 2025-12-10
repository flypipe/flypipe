from abc import ABC, abstractmethod


class Cache(ABC):
    """
    Superclass for Caches
    """

    def __init__(self):
        self.parent = None

    def set_parent(self, parent):
        self.parent = parent

    @abstractmethod
    def read(self, *args, **kwargs):
        """Read data from cache"""
        raise NotImplementedError("Cache subclass must implement read() method")

    @abstractmethod
    def write(self, *args, **kwargs):
        """Write data to cache"""
        raise NotImplementedError("Cache subclass must implement write() method")

    @abstractmethod
    def exists(self, *args, **kwargs):
        """Check if cached data exists"""
        raise NotImplementedError("Cache subclass must implement exists() method")
