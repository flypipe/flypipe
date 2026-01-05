import os
from uuid import uuid4

import pandas as pd
import pytest

from flypipe.cache import CacheMode
from flypipe.cache.cache import Cache
from flypipe.cache.cache_context import CacheContext
from flypipe.node import node


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


@pytest.fixture(scope="function")
def node_cache():
    @node(type="pandas", cache=GenericCache())
    def t0():
        return pd.DataFrame(data={"col1": [1]})

    return t0


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") != "CORE",
    reason="Core tests require RUN_MODE=CORE",
)
class TestCacheContextCore:
    """Unit tests on the CacheContext class - Core functionality (Pandas only)"""

    def test_create(self):
        """Test basic cache context creation"""
        cache_context = CacheContext()

        assert cache_context is not None
        assert cache_context.session is None
        assert cache_context.cache is None
        assert cache_context.disabled

    def test_disabled(self, node_cache):
        """Test CacheMode.DISABLE functionality"""
        cache_context = CacheContext(cache_mode=CacheMode.DISABLE, cache=node_cache)
        assert cache_context.disabled

    def test_merge(self, node_cache):
        """Test CacheMode.MERGE functionality"""
        cache_context = CacheContext(cache_mode=CacheMode.MERGE, cache=node_cache)
        assert cache_context.merge

    def test_write_read_exists(self):
        """Test basic write/read/exists operations with Pandas cache"""
        cache_context = CacheContext(cache=GenericCache())
        cache_context.write(pd.DataFrame(data={"col1": [1]}))
        cache_context.read()
        cache_context.exists()

    def test_exists(self):
        """Test exists() method with generic cache"""
        cache_context = CacheContext(cache=GenericCache())
        cache_context.exists()

    def test_exists_no_cache(self):
        """Test that exists() raises error when no cache is configured"""
        cache_context = CacheContext()
        with pytest.raises(RuntimeError):
            cache_context.exists()
