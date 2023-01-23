import os
from contextlib import contextmanager
import pytest
from flypipe.config import get_config, config_context, RunMode


class TestConfig:
    """Tests for Config"""

    @contextmanager
    def _set_environment_variable_for_test(self, name, value):
        """
        Simple context manager to ensure that any environment variable assignments are only for the duration of the
        test, otherwise they will leak into other tests.
        """
        old_value = os.environ.get(name)
        os.environ[name] = value
        try:
            yield
        finally:
            if old_value is None:
                os.environ.pop(name)
            else:
                os.environ[name] = old_value

    @pytest.mark.parametrize(
        "env_value,expected",
        [
            ("false", False),
            ("False", False),
            ("true", True),
            ("True", True),
            ("Bananas", "Bananas"),
        ],
    )
    def test_get_config_by_environment(self, env_value, expected):
        """
        Ensure that setting an appropriate environment variable name flows through to the config value and that boolean
        casts are done where appropriate.
        """
        with self._set_environment_variable_for_test(
            "FLYPIPE_REQUIRE_NODE_DESCRIPTION", env_value
        ):
            assert get_config("require_node_description") == expected

    def test_get_config_by_context_manager(self):
        """
        Here we ensure that:
        a) we can set a config var via context manager
        b) it takes precedence over the config being set via env variable
        c) once the context manager is destroyed the config setting from the context manager is also destroyed
        """
        with self._set_environment_variable_for_test(
            "FLYPIPE_REQUIRE_NODE_DESCRIPTION", "True"
        ):
            with config_context(require_node_description=False):
                assert get_config("require_node_description") is False
            assert get_config("require_node_description") is True

    def test_get_config_default(self):
        assert get_config("default_run_mode") == RunMode.SEQUENTIAL.value
