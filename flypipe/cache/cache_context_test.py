# import os
#
# import pandas as pd
# import pytest
#
# from flypipe.cache import CacheMode
# from flypipe.cache.cache_context import CacheContext
# from flypipe.cache.generic_cache import GenericCache
# from flypipe.node import node
#
#
# @pytest.fixture(autouse=True)
# def run_around_tests():
#     if os.path.exists("test.csv"):
#         os.remove("test.csv")
#
#     yield
#
#     if os.path.exists("test.csv"):
#         os.remove("test.csv")
#
#
# def read():
#     return pd.read_csv("test.csv")
#
#
# def write(df):
#     df.to_csv("test.csv", index=False)
#
#
# def exists():
#     return os.path.exists("test.csv")
#
#
# @pytest.fixture(scope="function")
# def node_cache():
#
#     @node(type="pandas", cache=GenericCache(read=read, write=write, exists=exists))
#     def t0(): # pylint: disable=duplicate-code)
#         return pd.DataFrame(data={"col1": [1]})
#
#     return t0
#
#
# class TestCacheContext:
#     """Unit tests on the Node class"""
#
#     def test_create(self, node_cache):
#         cache_context = CacheContext(None, "spark")
#         cache_context = cache_context.create(node_cache)
#
#         assert cache_context is not None
#         assert cache_context.spark == "spark"
#         assert cache_context.cache == node_cache.cache
#         assert not cache_context.disabled
#
#     def test_create_non_cached_node(self):
#         @node(type="pandas")
#         def t0():
#             return pd.DataFrame(data={"col1": [1]})
#
#         cache_context = CacheContext()
#         cache_context = cache_context.create(t0)
#         assert cache_context is None
#
#     def test_disabled(self, node_cache):
#         cache_mode = {
#             node_cache: CacheMode.DISABLE,
#         }
#
#         cache_context = CacheContext(cache_mode, "spark")
#         cache_context = cache_context.create(node_cache)
#         assert cache_context is None
#
#     def test_read(self, node_cache):
#         cache_context = CacheContext()
#         cache_context = cache_context.create(node_cache)
#         cache_context.write(pd.DataFrame(data={"col1": [1]}))
#         cache_context.read()
#
#     def test_write(self, node_cache):
#         cache_context = CacheContext()
#         cache_context = cache_context.create(node_cache)
#         cache_context.write(pd.DataFrame(data={"col1": [1]}))
#
#     def test_exists(self, node_cache):
#         cache_context = CacheContext()
#         cache_context = cache_context.create(node_cache)
#         cache_context.exists()
