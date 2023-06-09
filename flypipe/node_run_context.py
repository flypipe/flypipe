from flypipe.cache.cache import Cache
from flypipe.cache.cache_context import CacheContext


class NodeRunContext:  # pylint: disable=too-few-public-methods
    """
    NodeRunContext is a model held by each graph node that holds node information that is tied to a particular run,
    such as parameters.
    """

    def __init__(self, parameters=None, cache: CacheContext = None, provided_input=None):
        self._parameters = parameters or {}
        self._cache = cache or CacheContext()
        self._provided_input = provided_input
        self._cache = CacheContext() if self.exists_provided_input else self._cache

    @property
    def exists_provided_input(self):
        return self._provided_input is not None

    @property
    def provided_input(self):
        return self._provided_input

    @provided_input.setter
    def provided_input(self, provided_input):
        self._provided_input = provided_input
        self.cache = self._cache

    @property
    def cache(self):
        return self._cache

    @cache.setter
    def cache(self, cache_context: CacheContext):
        if self.exists_provided_input:
            self._cache = CacheContext()
        else:
            self._cache = cache_context

    @property
    def parameters(self) -> dict:
        return self._parameters

    @parameters.setter
    def parameters(self, parameters: dict):
        self._parameters = parameters
