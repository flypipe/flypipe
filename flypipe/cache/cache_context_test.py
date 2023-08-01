import os

import pandas as pd
import pytest

from flypipe.cache import CacheMode
from flypipe.cache.cache import Cache
from flypipe.cache.cache_context import CacheContext
from flypipe.node import node

# noinspection PyUnresolvedReferences
from flypipe.tests.conftest import spark


@pytest.fixture(autouse=True)
def run_around_tests():
    if os.path.exists("test.csv"):
        os.remove("test.csv")

    yield

    if os.path.exists("test.csv"):
        os.remove("test.csv")


# pylint: disable=missing-class-docstring
class GenericCache(Cache):

    # pylint: disable=arguments-differ
    def read(self):
        return pd.read_csv("test.csv")

    # pylint: disable=arguments-differ
    def write(self, df):
        df.to_csv("test.csv", index=False)

    # pylint: disable=arguments-differ
    def exists(self):
        return os.path.exists("test.csv")


class GenericCacheSpark(Cache):

    # pylint: disable=arguments-differ, unused-argument)
    def read(self, spark):
        return pd.read_csv("test.csv")

    # pylint: disable=arguments-differ, unused-argument)
    def write(self, spark, df):
        df.to_csv("test.csv", index=False)

    # pylint: disable=arguments-differ, unused-argument)
    def exists(self, spark):
        return os.path.exists("test.csv")


@pytest.fixture(scope="function")
def node_cache():
    @node(type="pandas", cache=GenericCache())
    def t0():  # pylint: disable=duplicate-code)
        return pd.DataFrame(data={"col1": [1]})

    return t0


class TestCacheContext:
    """Unit tests on the Node class"""

    def test_create(self):
        cache_context = CacheContext()

        assert cache_context is not None
        assert cache_context.spark is None
        assert cache_context.cache is None
        assert cache_context.disabled

    def test_disabled(self, node_cache):
        cache_context = CacheContext(cache_mode=CacheMode.DISABLE, cache=node_cache)
        assert cache_context.disabled

    def test_merge(self, node_cache):
        cache_context = CacheContext(cache_mode=CacheMode.MERGE, cache=node_cache)
        assert cache_context.merge

    def test_write_read_exists(self):
        cache_context = CacheContext(cache=GenericCache())
        cache_context.write(pd.DataFrame(data={"col1": [1]}))
        cache_context.read()
        cache_context.exists()

    def test_write_read_spark(self):
        cache_context = CacheContext(spark=spark, cache=GenericCacheSpark())
        cache_context.write(pd.DataFrame(data={"col1": [1]}))
        cache_context.read()
        cache_context.exists()

    def test_write_non_spark(self):
        cache_context = CacheContext(spark=spark, cache=GenericCache())

        with pytest.raises(TypeError):
            cache_context.write(pd.DataFrame(data={"col1": [1]}))

    def test_exists(self):
        cache_context = CacheContext(cache=GenericCache())
        cache_context.exists()

    def test_exists_spark(self):
        cache_context = CacheContext(spark=spark, cache=GenericCacheSpark())
        cache_context.exists()

    def test_exists_no_cache(self):
        cache_context = CacheContext()
        with pytest.raises(RuntimeError):
            cache_context.exists()

    def test_exists_no_spark_cache(self):
        cache_context = CacheContext(spark=spark, cache=GenericCache())
        with pytest.raises(TypeError):
            cache_context.exists()

    def test_exists_spark_cache_no_session(self):
        cache_context = CacheContext(cache=GenericCacheSpark())
        with pytest.raises(TypeError):
            cache_context.exists()
