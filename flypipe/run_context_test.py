import pandas as pd
from flypipe.dependency.preprocess_mode import PreprocessMode
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
