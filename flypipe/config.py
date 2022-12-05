import os
from contextlib import contextmanager
from enum import Enum


class RunMode(Enum):
    PARALLEL = 'parallel'
    SEQUENTIAL = 'sequential'


class _Config:
    OPTIONS = {
        'require_node_description': False,
        'require_schema_description': False,
        'default_run_mode': RunMode.SEQUENTIAL.value,
    }
    VALID_OPTIONS = set(config_name for config_name in OPTIONS.keys())
    ACTIVE_CONFIGS = []

    def __init__(self):
        self.config = {}

    @classmethod
    def get_config(cls, config_name):
        if config_name not in cls.OPTIONS:
            raise KeyError(f'Config option "{config_name}" is invalid, available options are {cls.VALID_OPTIONS}')
        active_config = cls.get_active_config()
        if active_config:
            return active_config._get_config(config_name)
        else:
            environment_config = cls._get_config_from_environment_variables(config_name)
            if environment_config is not None:
                return environment_config
            else:
                return cls.OPTIONS[config_name]

    @classmethod
    def _get_config_from_environment_variables(cls, config_name):
        environment_name = f'FLYPIPE_{config_name.upper()}'
        config = os.environ.get(environment_name)
        # Environment variables only support strings so we have to manually cast booleans from appropriate string
        # representations
        if config:
            if config.lower() == 'true':
                config = True
            elif config.lower() == 'false':
                config = False
        return config

    def _get_config(self, config_name):
        return self.config.get(config_name)

    def set_config(self, config_name, value):
        if config_name not in self.OPTIONS:
            raise KeyError(f'Config option "{config_name}" is invalid, available options are {self.VALID_OPTIONS}')
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
    try:
        yield
    finally:
        _Config.deregister()
