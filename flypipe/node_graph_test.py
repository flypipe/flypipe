import pandas as pd

from flypipe.cache import Cache
from flypipe.cache.cache_context import CacheContext
from flypipe.node import node
from flypipe.node_graph import NodeGraph, RunStatus
from flypipe.run_context import RunContext


class TestNodeGraph:
    """Tests for NodeGraph"""

    def test_build_graph(self):
        """
        Ensure an appropriate graph is built for a transformation
           T2
          // \\
        T1    T4
          \\ //
           T3
        """

        @node(type="pandas")
        def t1():
            return

        @node(type="pandas", dependencies=[t1.select("fruit")])
        def t2():
            return

        @node(type="pandas", dependencies=[t1.select("color")])
        def t3():
            return

        @node(type="pandas", dependencies=[t2.select("fruit"), t3.select("color")])
        def t4():
            return

        graph = NodeGraph(t4, run_context=RunContext())
        assert set(graph.get_edges()) == {
            (t1.key, t2.key),
            (t1.key, t3.key),
            (t2.key, t4.key),
            (t3.key, t4.key),
        }
        assert graph.get_edge_data(t1.key, t2.key)["selected_columns"] == ["fruit"]
        assert graph.get_edge_data(t1.key, t3.key)["selected_columns"] == ["color"]
        assert graph.get_node(t1.key)["output_columns"] == ["color", "fruit"]

    def test_calculate_graph_run_status_1(self):
        @node(type="pandas")
        def t1():
            return

        @node(type="pandas")
        def t2():
            return

        @node(type="pandas")
        def t3():
            return

        @node(
            type="pandas",
            dependencies=[t1.select("c1"), t2.select("c1"), t3.select("c1")],
        )
        def t4():
            return

        @node(type="pandas", dependencies=[t2.select("c1"), t3.select("c1")])
        def t5():
            return

        @node(type="pandas", dependencies=[t4.select("c1"), t5.select("c1")])
        def t6():
            return

        graph = NodeGraph(t6, RunContext(provided_inputs={t4: pd.DataFrame()}))

        assert graph.get_node(t1.key)["status"] == RunStatus.SKIP
        assert graph.get_node(t2.key)["status"] == RunStatus.ACTIVE
        assert graph.get_node(t3.key)["status"] == RunStatus.ACTIVE
        assert (
            graph.get_node(t4.key)["status"] == RunStatus.PROVIDED_INPUT
        )  # Changed from SKIP
        assert graph.get_node(t5.key)["status"] == RunStatus.ACTIVE
        assert graph.get_node(t6.key)["status"] == RunStatus.ACTIVE

    def test_calculate_graph_run_status_2(self):
        @node(type="pandas")
        def t1():
            return

        @node(type="pandas")
        def t2():
            return

        @node(type="pandas")
        def t3():
            return

        @node(
            type="pandas",
            dependencies=[t1.select("c1"), t2.select("c1"), t3.select("c1")],
        )
        def t4():
            return

        @node(type="pandas", dependencies=[t2.select("c1"), t3.select("c1")])
        def t5():
            return

        @node(type="pandas", dependencies=[t5.select("c1"), t4.select("c1")])
        def t6():
            return

        graph = NodeGraph(t6, RunContext(provided_inputs={t4: pd.DataFrame()}))

        assert graph.get_node(t1.key)["status"] == RunStatus.SKIP
        assert graph.get_node(t2.key)["status"] == RunStatus.ACTIVE
        assert graph.get_node(t3.key)["status"] == RunStatus.ACTIVE
        assert (
            graph.get_node(t4.key)["status"] == RunStatus.PROVIDED_INPUT
        )  # Changed from SKIP
        assert graph.get_node(t5.key)["status"] == RunStatus.ACTIVE
        assert graph.get_node(t6.key)["status"] == RunStatus.ACTIVE

    def test_get_nodes_depth(self):
        """
        t1 - t3 - t5
           / |
        t2 - t4
        """

        @node(type="pandas")
        def t1():
            return

        @node(type="pandas")
        def t2():
            return

        @node(type="pandas", dependencies=[t1.select("dummy"), t2.select("dummy")])
        def t3():
            return

        @node(type="pandas", dependencies=[t3.select("dummy"), t2.select("dummy")])
        def t4():
            return

        @node(type="pandas", dependencies=[t4.select("dummy")])
        def t5():
            return

        graph = NodeGraph(t5, run_context=RunContext())
        assert graph.get_nodes_depth() == {
            1: [t2.key, t1.key],
            2: [t3.key],
            3: [t4.key],
            4: [t5.key],
        }

    def test_get_cache_context_dependency_map(self):
        """
        Test that get_cache_context_dependency_map returns correct cache map
        Graph structure:
           t1 (with cache)
          /  \\
        t2    t3 (with cache)
          \\  /
           t4
        
        Only t1 and t3 should be in the map (they have caches)
        """

        class DummyCache(Cache):
            def __init__(self, name):
                self.name = name

            def read(self):
                return pd.DataFrame()

            def write(self, df):
                pass

            def exists(self):
                return False

        cache_t1 = DummyCache("cache_t1")
        cache_t3 = DummyCache("cache_t3")

        @node(type="pandas", cache=cache_t1)
        def t1():
            return pd.DataFrame({"col1": [1]})

        @node(type="pandas", dependencies=[t1])
        def t2(t1):
            return t1

        @node(type="pandas", dependencies=[t1], cache=cache_t3)
        def t3(t1):
            return t1

        @node(type="pandas", dependencies=[t2, t3])
        def t4(t2, t3):
            return t2

        graph = NodeGraph(t4, run_context=RunContext())
        dependency_map = graph.get_cache_context_dependency_map()

        # Verify only nodes with caches are in the map
        assert isinstance(dependency_map, dict)
        assert len(dependency_map) == 2  # Only t1 and t3 have caches

        # Verify t1 is in the map with its cache
        assert t1 in dependency_map
        assert isinstance(dependency_map[t1], CacheContext)
        assert dependency_map[t1].cache == cache_t1

        # Verify t3 is in the map with its cache
        assert t3 in dependency_map
        assert isinstance(dependency_map[t3], CacheContext)
        assert dependency_map[t3].cache == cache_t3

        # Verify t2 and t4 are NOT in the map (no caches)
        assert t2 not in dependency_map
        assert t4 not in dependency_map

    def test_get_cache_context_dependency_map_linear(self):
        """
        Test dependency map with a linear chain
        t1 (with cache) -> t2 -> t3 (with cache)
        """

        class DummyCache(Cache):
            def __init__(self, name):
                self.name = name

            def read(self):
                return pd.DataFrame()

            def write(self, df):
                pass

            def exists(self):
                return False

        cache_t1 = DummyCache("cache_t1")
        cache_t3 = DummyCache("cache_t3")

        @node(type="pandas", cache=cache_t1)
        def t1():
            return pd.DataFrame({"col1": [1]})

        @node(type="pandas", dependencies=[t1])
        def t2(t1):
            return t1

        @node(type="pandas", dependencies=[t2], cache=cache_t3)
        def t3(t2):
            return t2

        graph = NodeGraph(t3, run_context=RunContext())
        dependency_map = graph.get_cache_context_dependency_map()

        # Verify only nodes with caches are in the map
        assert isinstance(dependency_map, dict)
        assert len(dependency_map) == 2  # Only t1 and t3 have caches

        # Verify t1 is in the map
        assert t1 in dependency_map
        assert isinstance(dependency_map[t1], CacheContext)
        assert dependency_map[t1].cache == cache_t1

        # Verify t3 is in the map
        assert t3 in dependency_map
        assert isinstance(dependency_map[t3], CacheContext)
        assert dependency_map[t3].cache == cache_t3

        # Verify t2 is NOT in the map (no cache)
        assert t2 not in dependency_map
