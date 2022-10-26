_instance = None


# TODO- we should investigate if global variables like _instance are going to pose problems for us in spark before
# using this.
class NodeRunContext:
    VALID_KEYS = set(['pandas_on_spark_use_pandas'])

    def __init__(self, **kwargs):
        self.data = {k: None for k in self.VALID_KEYS}
        for k, v in kwargs.items():
            if k in self.VALID_KEYS:
                self.data[k] = v
            else:
                raise ValueError(f'Invalid context key "{k}"')

    @classmethod
    def get_instance(cls):
        if _instance:
            return _instance
        else:
            raise Exception('Unable to retrieve run context')

    def get_value(self, key):
        return self.data[key]

    def __enter__(self):
        global _instance
        _instance = self

    def __exit__(self, exception_type, exception_value, traceback):
        # TODO- gracefully handle exceptions
        global _instance
        _instance = None
