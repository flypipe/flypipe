import os
from uuid import uuid4

import pandas as pd
import pytest

from flypipe.cache import CacheMode
from flypipe.cache.cache import Cache
from flypipe.cache.cache_context import CacheContext


class GenericCache(Cache):
    """Generic Pandas-based cache for testing"""
    def __init__(self):
        self.cache_csv = f"{str(uuid4())}.csv"

    def read(self, from_node=None, to_node=None, is_static=False):
        return pd.read_csv(self.cache_csv)

    def write(
        self,
        df,
        upstream_nodes=None,
        to_node=None,
        datetime_started_transformation=None,
    ):
        df.to_csv(self.cache_csv, index=False)

    def exists(self):
        return os.path.exists(self.cache_csv)


class GenericCacheSpark(Cache):
    """Generic PySpark-compatible cache for testing"""
    def __init__(self):
        self.cache_csv = f"{str(uuid4())}.csv"

    def read(self, session, from_node=None, to_node=None, is_static=False):
        return pd.read_csv(self.cache_csv)

    def write(
        self,
        session,
        df,
        upstream_nodes=None,
        to_node=None,
        datetime_started_transformation=None,
    ):
        df.to_csv(self.cache_csv, index=False)

    def exists(self, session):
        return os.path.exists(self.cache_csv)


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") not in ["SPARK", "SPARK_CONNECT"],
    reason="PySpark tests require RUN_MODE=SPARK or RUN_MODE=SPARK_CONNECT",
)
class TestCacheContextPySpark:
    """Unit tests on the CacheContext class - PySpark functionality"""

    def test_write_read_with_session(self, spark):
        """Test write/read/exists operations with PySpark cache"""
        cache_context = CacheContext(session=spark, cache=GenericCacheSpark())
        cache_context.write(pd.DataFrame(data={"col1": [1]}))
        cache_context.read()
        cache_context.exists()

    def test_write_incompatible_cache(self, spark):
        """Test that using non-session cache with session raises error"""
        cache_context = CacheContext(session=spark, cache=GenericCache())

        with pytest.raises(TypeError):
            cache_context.write(pd.DataFrame(data={"col1": [1]}))

    def test_exists_with_session(self, spark):
        """Test exists() method with session-based cache"""
        cache_context = CacheContext(session=spark, cache=GenericCacheSpark())
        cache_context.exists()

    def test_exists_incompatible_cache(self, spark):
        """Test that exists() raises error when using non-session cache with session"""
        cache_context = CacheContext(session=spark, cache=GenericCache())
        with pytest.raises(TypeError):
            cache_context.exists()

    def test_exists_session_cache_no_session(self):
        """Test that exists() raises error when session cache has no session"""
        cache_context = CacheContext(cache=GenericCacheSpark())
        with pytest.raises(TypeError):
            cache_context.exists()

