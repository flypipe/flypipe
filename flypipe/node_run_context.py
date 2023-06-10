from flypipe.cache import CacheMode
from flypipe.cache.cache import Cache
from flypipe.cache.cache_context import CacheContext

"""


"""
class NodeRunContext:  # pylint: disable=too-few-public-methods
    """
    NodeRunContext is a model held by each graph node that holds node information that is tied to a particular run,
    such as parameters.
    """

    def __init__(self, parameters=None, cache_context: CacheContext = None, provided_input=None):
        self._parameters = parameters or {}
        self._provided_input = provided_input
        self._cache_context = cache_context or CacheContext()

    @property
    def exists_provided_input(self):
        return self._provided_input is not None

    @property
    def provided_input(self):
        return self._provided_input

    @property
    def cache_context(self):
        return self._cache_context

    @cache_context.setter
    def cache_context(self, cache_context: CacheContext):
        self._cache_context = cache_context

    @property
    def parameters(self) -> dict:
        return self._parameters

    @parameters.setter
    def parameters(self, parameters: dict):
        self._parameters = parameters
