from abc import ABC, abstractmethod
from flypipe.cache import Cache


class CDCCache(Cache, ABC):
    """
    Superclass for CDCCaches
    """

    @abstractmethod
    def create_cdc_table(self, *args, **kwargs):
        """Ensure CDC metadata table exists (for thread-safe parallel execution)"""
        raise NotImplementedError(
            "CDCCache subclass must implement create_cdc_table() method"
        )

    @abstractmethod
    def read_cdc(self, *args, **kwargs):
        """Read CDC metadata to filter data based on what has already been processed"""
        raise NotImplementedError("CDCCache subclass must implement read_cdc() method")

    @abstractmethod
    def write_cdc(self, *args, **kwargs):
        """Write CDC metadata to track what data has been processed"""
        raise NotImplementedError("CDCCache subclass must implement write_cdc() method")
