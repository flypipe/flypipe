import logging
from pyspark.sql import SparkSession

from flypipe.cache import CacheMode, CDCCache, Cache
from flypipe.utils import log

logger = logging.getLogger(__name__)


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

    def _log(self, message: str):
        """Log a message using logger.debug if debug mode is enabled, otherwise print."""
        if self.debug:
            log(logger, message)

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

    def read(self):
        if self.disabled:
            raise RuntimeError("Cache disabled, cannot read")

        self._log("      🔄 CacheContext.read() - calling cache.read()")
        if self.spark:
            result = self.cache.read(self.spark)
        else:
            result = self.cache.read()
        self._log("      🔄 CacheContext.read() - returned result")
        return result

    def read_cdc(self, from_node, to_node, df):
        if not self.disabled and isinstance(self.cache, CDCCache):
            self._log(
                f"      📊 CacheContext.read_cdc() - filtering CDC data from {from_node.__name__} to {to_node.__name__}"
            )
            if self.spark:
                result = self.cache.read_cdc(self.spark, from_node, to_node, df)
            else:
                result = self.cache.read_cdc(from_node, to_node, df)
            return result
        else:
            if self.disabled:
                self._log("      ⏭️  CacheContext.read_cdc() - skipped (cache disabled)")
            else:
                self._log("      ⏭️  CacheContext.read_cdc() - skipped (not a CDCCache)")
            return df

    def write(self, df):
        if not self.disabled or self.merge:
            mode = "MERGE" if self.merge else "NORMAL"
            self._log(
                f"      💾 CacheContext.write() - mode: {mode}, calling cache.write()"
            )
            if self.spark:
                result = self.cache.write(self.spark, df)
            else:
                result = self.cache.write(df)
            self._log("      💾 CacheContext.write() - write complete")
            return result
        else:
            self._log("      ⏭️  CacheContext.write() - skipped (cache disabled)")
            return None

    def create_cdc_table(self):
        """
        Ensure CDC metadata table exists.

        This should be called before parallel execution to avoid concurrent
        table creation conflicts.
        """
        if isinstance(self.cache, CDCCache):
            if self.spark:
                self.cache.create_cdc_table(self.spark)
            else:
                self.cache.create_cdc_table()

    def write_cdc(self, upstream_nodes, to_node, datetime_started_transformation):
        if isinstance(self.cache, CDCCache):
            if not self.disabled or self.merge:
                upstream_names = [
                    n.__name__ if hasattr(n, "__name__") else str(n)
                    for n in upstream_nodes
                ]
                self._log(
                    f"      📝 CacheContext.write_cdc() - writing CDC metadata for {to_node.__name__}"
                )
                self._log(
                    f"         └─ upstream nodes: {', '.join(upstream_names) if upstream_names else 'none'}"
                )
                if not upstream_nodes:
                    self._log(
                        "            ⏭️ No upstream nodes to write CDC metadata for, skipping"
                    )
                else:
                    for upstream_node in upstream_nodes:
                        if self.spark:
                            self.cache.write_cdc(
                                self.spark,
                                upstream_node,
                                to_node,
                                datetime_started_transformation,
                            )
                        else:
                            self.cache.write_cdc(
                                upstream_node,
                                to_node,
                                datetime_started_transformation,
                            )

            else:
                self._log(
                    "      ⏭️  CacheContext.write_cdc() - skipped (cache disabled)"
                )
        else:
            self._log("      ⏭️  CacheContext.write_cdc() - skipped (not a CDCCache)")

    def exists(self):
        if self.disabled:
            raise RuntimeError("Cache disabled, cannot check if exists")

        if self.spark:
            self._exists_cache_to_load = self.cache.exists(self.spark)
        else:
            self._exists_cache_to_load = self.cache.exists()
        return self._exists_cache_to_load
