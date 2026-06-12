import pandas as pd
from flypipe.cache import CacheMode
from flypipe.cache.cache import Cache
from flypipe.dependency.preprocess_mode import PreprocessMode
from flypipe.node import node
from flypipe.run_context import RunContext
from flypipe.runner import Runner


def _run_with_inspectable_context(target, run_context):
    """Execute ``target`` exactly the way Node.run does internally, but with a
    caller-supplied RunContext so tests can inspect it after the run."""
    target.create_graph(run_context)
    execution_graph = target.node_graph.get_execution_graph(run_context)
    Runner(node_graph=execution_graph, run_context=run_context).run(target_node=target)
    return run_context.get_graph_result(target)


@node(type="pandas")
def t1():
    return pd.DataFrame()


@node(type="pandas", dependencies=[t1])
def t2(t1):
    return t1


class InMemoryCache(Cache):
    """Minimal cache for exercising cached/MERGE flows without touching disk."""

    def __init__(self):
        self.df = None

    def read(self, from_node=None, to_node=None, is_static=False):
        return self.df

    def write(
        self,
        df,
        upstream_nodes=None,
        to_node=None,
        datetime_started_transformation=None,
    ):
        self.df = df

    def exists(self):
        return self.df is not None


def _make_identity_transform(i):
    """Build a uniquely-named identity transform (distinct __name__ -> distinct
    node key) for assembling a serial chain in tests."""

    def transform(df):
        return df

    transform.__name__ = f"chain_{i}"
    return transform


def _build_identity_chain(num_nodes):
    """chain_0 -> chain_1 -> ... -> chain_{num_nodes-1}; returns the tail node."""

    @node(type="pandas")
    def chain_0():
        return pd.DataFrame({"x": [1, 2, 3]})

    chain_0.name = "chain_0"
    nodes = [chain_0]
    for i in range(1, num_nodes):
        prev = nodes[-1]
        transform = _make_identity_transform(i)
        nodes.append(node(type="pandas", dependencies=[prev.alias("df")])(transform))
    return nodes[-1]


class TestRunContext:
    """Tests on Nodes with pyspark type"""

    def test_get_dependency_preprocess_mode_defaults_to_active(self):
        run_context = RunContext()

        assert (
            run_context.get_dependency_preprocess_mode(t2, t1) == PreprocessMode.ACTIVE
        )

    def test_get_dependency_preprocess_mode_globally_disabled(self):
        run_context = RunContext(dependencies_preprocess_modes=PreprocessMode.DISABLE)

        assert (
            run_context.get_dependency_preprocess_mode(t2, t1) == PreprocessMode.DISABLE
        )

    def test_get_dependency_preprocess_mode_locally_disabled(self):
        run_context = RunContext(
            dependencies_preprocess_modes={t2: {t1: PreprocessMode.DISABLE}}
        )

        assert (
            run_context.get_dependency_preprocess_mode(t2, t1) == PreprocessMode.DISABLE
        )

    def test_release_node_result_removes_entry(self):
        run_context = RunContext()
        run_context.update_node_results(t1, t2, pd.DataFrame({"x": [1]}))
        assert t1 in run_context.node_results
        assert t2 in run_context.node_results[t1]

        run_context.release_node_result(t1, t2)

        # The inner entry is dropped; the outer key stays (a concurrent MERGE
        # sub-run may be re-populating it under its own target key).
        assert t2 not in run_context.node_results[t1]

    def test_release_node_result_missing_is_noop(self):
        run_context = RunContext()
        # Must not raise and must not auto-create an Autodict entry.
        run_context.release_node_result(t1, t2)
        assert t1 not in run_context.node_results


class TestReleaseIntermediate:
    """Tests for the mid-run eviction of intermediate node results."""

    def test_returns_correct_result(self):
        result = _build_identity_chain(6).run()

        assert result["x"].tolist() == [1, 2, 3]

    def test_frees_intermediate_results_during_run(self):
        tail = _build_identity_chain(6)

        # Measure (do NOT evict) the peak number of stored results by observing
        # node_results after each store. The library does the eviction.
        peak = {"max": 0}
        original = RunContext.update_node_results

        def measured(self, from_node, to_node, df):
            original(self, from_node, to_node, df)
            held = sum(len(inner) for inner in self.node_results.values())
            peak["max"] = max(peak["max"], held)

        RunContext.update_node_results = measured
        try:
            result = tail.run()
        finally:
            RunContext.update_node_results = original

        # Only the in-flight working set (current input + current output) is ever
        # held, regardless of chain length.
        assert peak["max"] <= 2
        assert result["x"].tolist() == [1, 2, 3]

    def test_evicts_through_merge_cache_recursion(self):
        """A CacheMode.MERGE node mid-graph re-enters Runner.run with itself as
        target; eviction must work independently in both the outer and the
        inner run, and the inner run's own result (only needed to write the
        merged cache) must be freed when it ends."""

        @node(type="pandas")
        def source():
            return pd.DataFrame({"x": [1, 2, 3]})

        @node(type="pandas", dependencies=[source.alias("df")], cache=InMemoryCache())
        def cached_mid(df):
            return df

        @node(type="pandas", dependencies=[cached_mid.alias("df")])
        def downstream(df):
            return df

        @node(type="pandas", dependencies=[downstream.alias("df")])
        def tail(df):
            return df

        run_context = RunContext(cache_modes={cached_mid: CacheMode.MERGE})

        # Track the peak number of simultaneously stored results; this needs a
        # hook (mid-run state cannot be reconstructed after the run), hence the
        # try/finally to guarantee the class-level patch is undone.
        peak = {"max": 0}
        original = RunContext.update_node_results

        def measured(self, from_node, to_node, df):
            original(self, from_node, to_node, df)
            held = sum(len(inner) for inner in self.node_results.values())
            peak["max"] = max(peak["max"], held)

        RunContext.update_node_results = measured
        try:
            result = _run_with_inspectable_context(tail, run_context)
        finally:
            RunContext.update_node_results = original

        assert result["x"].tolist() == [1, 2, 3]
        # 6 results get stored across the outer + inner run (source twice, the
        # merge node twice, downstream, tail); eviction keeps at most 3 alive.
        assert peak["max"] <= 3
        # After the run nothing survives -- even the target's result is
        # released by get_graph_result when it is handed back to the caller.
        held_at_end = sum(len(inner) for inner in run_context.node_results.values())
        assert held_at_end == 0

    def test_merge_run_holds_no_dataframes_after_run(self):
        """A -> B (cached) -> C, run from C with B merged: once run() returns,
        run_context must hold no dataframes at all -- every intermediate is
        evicted mid-run and the target's own result is released by
        get_graph_result when it is handed back to the caller."""

        @node(type="pandas")
        def A():
            return pd.DataFrame({"x": [1, 2, 3]})

        @node(type="pandas", dependencies=[A.alias("df")], cache=InMemoryCache())
        def B(df):
            return df

        @node(type="pandas", dependencies=[B.alias("df")])
        def C(df):
            return df

        run_context = RunContext(cache_modes={B: CacheMode.MERGE})
        result = _run_with_inspectable_context(C, run_context)

        assert result["x"].tolist() == [1, 2, 3]
        held_at_end = sum(len(inner) for inner in run_context.node_results.values())
        assert held_at_end == 0

    def test_evicts_through_merge_cache_recursion_parallel(self):
        """With max_workers > 1 a MERGE sub-run executes inside a worker thread,
        concurrently with the rest of its level and with the outer run's
        releases; results and eviction must stay correct."""

        @node(type="pandas")
        def source():
            return pd.DataFrame({"x": [1, 2, 3]})

        @node(type="pandas", dependencies=[source.alias("df")])
        def left(df):
            return df

        @node(type="pandas", dependencies=[source.alias("df")], cache=InMemoryCache())
        def cached_mid(df):
            return df

        @node(
            type="pandas",
            dependencies=[left.alias("a"), cached_mid.alias("b")],
        )
        def tail(a, b):
            return a + b

        run_context = RunContext(
            max_workers=2, cache_modes={cached_mid: CacheMode.MERGE}
        )
        result = _run_with_inspectable_context(tail, run_context)

        assert result["x"].tolist() == [2, 4, 6]
        # After the run nothing survives -- even the target's result is
        # released by get_graph_result when it is handed back to the caller.
        held_at_end = sum(len(inner) for inner in run_context.node_results.values())
        assert held_at_end == 0
