import os
from contextlib import contextmanager

from flypipe.schema.util import DateFormat


class _Config:
    OPTIONS = {
        "catalog_count_box_tags": "bronze,silver,gold",
        "node_run_max_workers": 1,
        "require_node_description": False,
        "require_schema_description": False,
        "default_dependencies_preprocess_module": None,
        "default_dependencies_preprocess_function": None,
        "default_date_format_style": DateFormat.PYSPARK.value,  # "PYSPARK"
        "default_date_format": DateFormat.PYSPARK.date_format(),  # "yyyy-MM-dd"
        "default_datetime_format": DateFormat.PYSPARK.datetime_format(),  # "yyyy-MM-dd H:m:s"
    }
    VALID_OPTIONS = set(config_name for config_name in OPTIONS.keys())
    ACTIVE_CONFIGS = []

    def __init__(self):
        self.config = {}

    @classmethod
    def get_config(cls, config_name):
        """
        Retrieve the value of a Flypipe configuration variable. In order of precedence this comes from:
        - Set config via the config_context context manager
        - Corresponding environment variable for a config
        - Default config value (derived from date_format_style if applicable)
        """
        if config_name not in cls.OPTIONS:
            raise KeyError(
                f'Config option "{config_name}" is invalid, available options are {cls.VALID_OPTIONS}'
            )
        active_config = cls.get_active_config()
        if active_config:
            config_value = active_config.get_config_from_context_manager(config_name)
            if config_value is not None:
                return config_value
        environment_config = cls._get_config_from_environment_variables(config_name)
        if environment_config is not None:
            return environment_config
        
        # For date/datetime format, derive from current style if not explicitly overridden
        if config_name in ("default_date_format", "default_datetime_format"):
            # Check if the format was explicitly set (not in context or env, we already checked those)
            # If default_date_format_style was overridden, derive format from that style
            style = cls.get_config("default_date_format_style")
            # Convert string to DateFormat enum if needed
            if isinstance(style, str):
                style = DateFormat(style)
            
            if config_name == "default_date_format":
                return style.date_format()
            else:  # default_datetime_format
                return style.datetime_format()
        
        # Get default value
        return cls.OPTIONS[config_name]

    @classmethod
    def _get_config_from_environment_variables(cls, config_name):
        environment_name = f"FLYPIPE_{config_name.upper()}"
        config = os.environ.get(environment_name)
        # Environment variables only support strings so we have to manually cast booleans from appropriate string
        # representations
        if config:
            if config.lower() == "true":
                config = True
            elif config.lower() == "false":
                config = False
        return config

    def get_config_from_context_manager(self, config_name):
        return self.config.get(config_name)

    def set_config(self, config_name, value):
        if config_name not in self.OPTIONS:
            raise KeyError(
                f'Config option "{config_name}" is invalid, available options are {self.VALID_OPTIONS}'
            )
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
