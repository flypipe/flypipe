import os
from contextlib import contextmanager


class _Config:
    _OPTIONS = [
        'require_node_description',
    ]
    ACTIVE_CONFIGS = []

    def __init__(self):
        self.config = {}

    @classmethod
    def get_config(cls, config_name):
        if config_name not in cls._OPTIONS:
            raise KeyError(f'Config option "{config_name}" is invalid, available options are {cls._OPTIONS}')
        active_config = cls.get_active_config()
        if active_config:
            return active_config._get_config(config_name)
        else:
            return cls._get_config_from_environment_variables(config_name)

    @classmethod
    def _get_config_from_environment_variables(cls, config_name):
        environment_name = f'FLYPIPE_{config_name.upper()}'
        config = os.environ.get(environment_name)
        # Environment variables only support strings so we have to manually cast booleans from appropriate string
        # representations
        if config == 'True':
            config = True
        elif config == 'False':
            config = False
        return config

    def _get_config(self, config_name):
        return self.config.get(config_name)

    def set_config(self, config_name, value):
        if config_name not in self._OPTIONS:
            raise KeyError(f'Config option "{config_name}" is invalid, available options are {cls._OPTIONS}')
        self.config[config_name] = value

    @classmethod
    def register(cls, config):
        cls.ACTIVE_CONFIGS.append(config)

    @classmethod
    def deregister(cls):
        cls.ACTIVE_CONFIGS.pop()

    @classmethod
    def get_active_config(cls):
        if cls.ACTIVE_CONFIGS:
            return cls.ACTIVE_CONFIGS[-1]
        else:
            return None


def get_config(config_name):
    return _Config.get_config(config_name)


@contextmanager
def config_context(**kwargs):
    config = _Config()
    for k, v in kwargs.items():
        config.set_config(k, v)
    _Config.register(config)
    yield
    _Config.deregister()
