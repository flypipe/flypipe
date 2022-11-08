import os
from flypipe.config import get_config, config_context


class TestConfig:

    def test_set_config_by_environment(self):
        os.environ["FLYPIPE_REQUIRE_NODE_DESCRIPTION"] = 'True'

        assert get_config('require_node_description') is True

    def test_set_config_by_context_manager(self):
        """
        Here we ensure that:
        a) we can set a config var via context manager
        b) it takes precedence over the config being set via env variable
        c) once the context manager is destroyed the config setting from the context manager is also destroyed
        """
        os.environ["FLYPIPE_REQUIRE_NODE_DESCRIPTION"] = 'True'
        with config_context(require_node_description=False):
            assert get_config('require_node_description') is False
        assert get_config('require_node_description') is True
