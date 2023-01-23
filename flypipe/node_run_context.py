class NodeRunContext:  # pylint: disable=too-few-public-methods
    """
    NodeRunContext is a model held by each graph node that holds node information that is tied to a particular run,
    such as parameters.
    """

    def __init__(self, parameters=None):
        self._parameters = parameters or {}

    @property
    def parameters(self) -> dict:
        return self._parameters

    @parameters.setter
    def parameters(self, parameters: dict):
        self._parameters = parameters
