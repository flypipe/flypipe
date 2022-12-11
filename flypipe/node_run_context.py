class NodeRunContext:
    def __init__(self, parameters=None):
        self._parameters = parameters or {}

    @property
    def parameters(self) -> dict:
        return self._parameters

    @parameters.setter
    def parameters(self, parameters: dict):
        self._parameters = parameters
