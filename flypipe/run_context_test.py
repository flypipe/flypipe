import pandas as pd
from flypipe.dependency import PreProcessMode
from flypipe.node import node
from flypipe.run_context import RunContext


@node(type="pandas")
def t1():
    return pd.DataFrame()


@node(type="pandas", dependencies=[t1])
def t2(t1):
    return t1


class TestRunContext:
    """Tests on Nodes with pyspark type"""

    def test_get_run_preprocess_mode_set_on_nodes_returns_active(self):
        run_context = RunContext(
            dependencies_preprocess_modes={t2: {t1: PreProcessMode.DISABLE}}
        )

        assert (
            run_context.get_run_preprocess_mode().value == PreProcessMode.ACTIVE.value
        )

    def test_get_run_preprocess_mode_not_set_returns_active(self):
        run_context = RunContext()

        assert (
            run_context.get_run_preprocess_mode().value == PreProcessMode.ACTIVE.value
        )

    def test_get_run_preprocess_mode_disabled_returns_disabled(self):
        run_context = RunContext(dependencies_preprocess_modes=PreProcessMode.DISABLE)

        assert (
            run_context.get_run_preprocess_mode().value == PreProcessMode.DISABLE.value
        )

    def test_get_run_preprocess_mode_active_returns_active(self):
        run_context = RunContext(dependencies_preprocess_modes=PreProcessMode.ACTIVE)

        assert (
            run_context.get_run_preprocess_mode().value == PreProcessMode.ACTIVE.value
        )

    def test_get_dependency_preprocess_mode_active_if_nothing_is_set(self):
        run_context = RunContext()

        assert (
            run_context.get_dependency_preprocess_mode(t2, t2.input_nodes[0]).value
            == PreProcessMode.ACTIVE.value
        )

    def test_get_dependency_preprocess_mode_disabled_if_disabled(self):
        run_context = RunContext(
            dependencies_preprocess_modes={t2: {t1: PreProcessMode.DISABLE}}
        )

        assert (
            run_context.get_dependency_preprocess_mode(t2, t2.input_nodes[0]).value
            == PreProcessMode.DISABLE.value
        )

    def test_get_dependency_preprocess_mode_active_if_active(self):
        run_context = RunContext(
            dependencies_preprocess_modes={t2: {t1: PreProcessMode.ACTIVE}}
        )

        assert (
            run_context.get_dependency_preprocess_mode(t2, t2.input_nodes[0]).value
            == PreProcessMode.ACTIVE.value
        )
