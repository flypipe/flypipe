from pyspark.sql import SparkSession

from flypipe.cache import CacheMode, Cache
from flypipe.utils import get_logger

logger = get_logger()


class CacheContext:
    def __init__(
        self,
        cache_mode: CacheMode = None,
        spark: SparkSession = None,
        cache: Cache = None,
        debug: bool = False,
    ):
        self.spark = spark
        self.cache = cache
        self.cache_mode = CacheMode.DISABLE if self.cache is None else cache_mode
        self.debug = debug
        self._exists_cache_to_load = (
            None  # can not be True or False as both means exist or not exist
        )

    @property
    def disabled(self):
        return self.cache_mode == CacheMode.DISABLE

    @property
    def merge(self):
        return self.cache_mode == CacheMode.MERGE

    @property
    def exists_cache_to_load(self):
        if self.disabled:
            return False

        if self.merge:
            return False

        # this avoids calling self.exits() multiple times by NodeGraph._calculate_run_status method
        if self._exists_cache_to_load is not None:
            return self._exists_cache_to_load

        return self.exists()

    def create_cdc_table(self):
        """
        Ensure CDC metadata table exists.

        This should be called before parallel execution to avoid concurrent
        table creation conflicts.
        """
        if self.spark:
            self.cache.create_cdc_table(self.spark)
        else:
            self.cache.create_cdc_table()

    def read(self, from_node=None, to_node=None, is_static=False):
        if self.disabled:
            raise RuntimeError("Cache disabled, cannot read")

        if from_node is not None and to_node is not None:
            static_marker = "static " if is_static else ""
            logger.debug(
                f"              📤 CacheContext.read() - {static_marker}read and filter data from {from_node.__name__} to {to_node.__name__}"
            )
        else:
            logger.debug("              📤 CacheContext.read() - calling cache.read()")

        if self.spark:
            result = self.cache.read(
                self.spark, from_node=from_node, to_node=to_node, is_static=is_static
            )
        else:
            result = self.cache.read(
                from_node=from_node, to_node=to_node, is_static=is_static
            )
        return result

    def write(
        self,
        df,
        upstream_nodes=None,
        to_node=None,
        datetime_started_transformation=None,
    ):
        if not self.disabled or self.merge:
            # CDC metadata write (with upstream_nodes, to_node, datetime_started_transformation)

            if to_node is not None:
                logger.debug(
                    f"      📝 CacheContext.write() - writing CDC metadata for {to_node.__name__}"
                )

            if upstream_nodes:
                upstream_names = [n.__name__ for n in upstream_nodes]
                logger.debug(
                    f"         └─ upstream nodes: {', '.join(upstream_names) if upstream_names else 'none'}"
                )

            # Call unified write method with CDC parameters
            if self.spark:
                self.cache.write(
                    self.spark,
                    df=df,
                    upstream_nodes=upstream_nodes,
                    to_node=to_node,
                    datetime_started_transformation=datetime_started_transformation,
                )
            else:
                self.cache.write(
                    df=df,
                    upstream_nodes=upstream_nodes,
                    to_node=to_node,
                    datetime_started_transformation=datetime_started_transformation,
                )
        else:
            logger.debug("      ⏭️  CacheContext.write() - skipped (cache disabled)")
            return None

    def exists(self):
        if self.disabled:
            raise RuntimeError("Cache disabled, cannot check if exists")

        if self.spark:
            self._exists_cache_to_load = self.cache.exists(self.spark)
        else:
            self._exists_cache_to_load = self.cache.exists()
        return self._exists_cache_to_load
