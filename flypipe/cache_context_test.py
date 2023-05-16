import pandas as pd

from flypipe import node_function
from flypipe.cache_context import CacheContext
from flypipe.node import node


class TestCacheContext:
    """Unit tests on the Node class"""

    def test_cache_context(self):
        @node(type="pandas")
        def t0():
            return pd.DataFrame(data={"col1": [1]})

        @node_function()
        def tf():
            @node(type="pandas")
            def t1():
                return pd.DataFrame(data={"col1": [1]})

            return t1

        cache = {"disable": [t0, tf]}

        cache_context = CacheContext(cache)

        assert cache_context._disable == [t0, tf]  # pylint: disable=protected-access

    def test_is_disabled(self):
        @node(type="pandas")
        def t0():
            return pd.DataFrame(data={"col1": [1]})

        @node_function(node_dependencies=[t0])
        def tf():
            @node(type="pandas")
            def t1():
                return pd.DataFrame(data={"col1": [1]})

            return t1

        cache = {"disable": [t0]}

        cache_context = CacheContext(cache)

        assert cache_context.is_disabled(t0)
        assert not cache_context.is_disabled(tf)
