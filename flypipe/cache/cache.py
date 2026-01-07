from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from flypipe.node import Node

from flypipe.utils import get_logger

logger = get_logger()


class Cache(ABC):
    """
    Superclass for Caches with optional CDC (Change Data Capture) support.

    The Cache class now supports both regular caching and CDC functionality
    through unified read/write methods with optional CDC parameters.
    """

    def __init__(self):
        self.parent = None

    def set_parent(self, parent):
        self.parent = parent

    @abstractmethod
    def read(
        self,
        from_node: "Node" = None,
        to_node: "Node" = None,
        is_static: bool = False,
        *args,
        **kwargs,
    ):
        """
        Read data from cache with optional CDC filtering.

        Parameters
        ----------
        from_node : Node, optional
            Source node for CDC filtering
        to_node : Node, optional
            Destination node for CDC filtering
        is_static : bool, optional
            If True, skip CDC filtering and load complete cached data (default: False)
        *args, **kwargs
            Additional cache-specific parameters (e.g., spark session)

        Returns
        -------
        DataFrame
            Cached data, optionally filtered by CDC metadata
        """
        raise NotImplementedError("Cache subclass must implement read() method")

    @abstractmethod
    def write(
        self,
        *args,
        df,
        upstream_nodes: List["Node"] = None,
        to_node: "Node" = None,
        datetime_started_transformation: datetime = None,
        **kwargs,
    ):
        """
        Write data to cache with optional CDC metadata.

        Parameters
        ----------
        *args
            Additional positional arguments (e.g., spark session for Spark caches)
        df : DataFrame
            DataFrame to cache
        upstream_nodes : List[Node], optional
            List of upstream cached nodes for CDC tracking
        to_node : Node, optional
            Destination node for CDC tracking
        datetime_started_transformation : datetime, optional
            Timestamp when transformation started for CDC tracking
        **kwargs
            Additional keyword arguments
        """
        raise NotImplementedError("Cache subclass must implement write() method")

    @abstractmethod
    def exists(self, *args, **kwargs):
        """Check if cached data exists"""
        raise NotImplementedError("Cache subclass must implement exists() method")

    def create_cdc_table(self, *args, **kwargs):
        """
        Ensure CDC metadata table exists (optional for caches with CDC support).

        This method should be implemented by cache subclasses that support CDC.
        Default implementation does nothing (no-op).
        """
        logger.debug(
            f"         🔧 Cache.create_cdc_table() - default no-op implementation for {self.__class__.__name__} (override in subclass for CDC support)"
        )
