class CacheContext:
    def __init__(self, context):
        self._disable = [] if "disable" not in context else context["disable"]
        self._disable = (
            [self._disable] if not isinstance(self._disable, list) else self._disable
        )

    def is_disabled(self, node):
        return node in self._disable

    def __repr__(self):
        import json

        return json.dumps(
            {"disable": [node.function.__name__ for node in self._disable]}
        )
