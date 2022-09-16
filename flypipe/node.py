import logging
from concurrent.futures import ThreadPoolExecutor, wait
from flypipe.node_graph import NodeGraph
from collections import namedtuple

from flypipe.spark.dataframe import SparkDataframe

logger = logging.getLogger(__name__)


NodeResult = namedtuple('NamedTuple', ['node_type', 'value'])


class Node:

    TYPES = {}

    def __init__(self, transformation, inputs=None, type=None):
        self.transformation = transformation
        self.inputs = inputs or []
        self._type = None
        self.node_type = type
        self.node_graph = NodeGraph(self)

    @property
    def node_type(self):
        return self._type

    @node_type.setter
    def node_type(self, value):
        if value not in self.TYPES.keys():
            raise ValueError(f'Attempted to set invalid type "{value}", type must be one of {list(self.TYPES.keys())}')
        self._type = value
        # if value not in self.MODES.keys():
        #     raise ValueError(f'Attempted to set invalid mode "{value}", mode must be one of {list(self.MODES.keys())}')
        # self._mode = value

    @classmethod
    def register_type(cls, name, type_class):
        cls.TYPES[name] = type_class

    @property
    def __name__(self):
        """Return the name of the wrapped transformation rather than the name of the decorator object"""
        return self.transformation.__name__

    @property
    def __doc__(self):
        """Return the docstring of the wrapped transformation rather than the docstring of the decorator object"""
        return self.transformation.__doc__

    def __call__(self, *args, **kwargs):
        return self.transformation(*args, **kwargs)

    def run(self):
        outputs = {}
        dependency_chain = self.node_graph.get_dependency_chain()

        def process_and_cache_node(node_obj, **inputs):
            outputs[node_obj.__name__] = NodeResult(node_obj.node_type, self.process_node(node_obj, **inputs))

        with ThreadPoolExecutor() as executor:
            for node_group in dependency_chain:
                logger.debug(f'Started processing group of nodes {node_group}')
                result_futures = []
                for node_name in node_group:
                    node_obj = self.node_graph.get_node(node_name)
                    try:
                        node_inputs = {}
                        for input_node in node_obj.inputs:
                            # TODO type conversion needs to be a bit more sophisticated
                            if self.node_type == 'pandas' and outputs[input_node.__name__].node_type == 'spark':
                                input_value = SparkDataframe.to_pandas(outputs[input_node.__name__].value)
                            else:
                                input_value = outputs[input_node.__name__].value
                            node_inputs[input_node.__name__] = input_value
                    except KeyError as ex:
                        raise ValueError(f'Unable to process transformation {node_obj.__name__}, missing input {ex.args[0]}')

                    logger.debug(f'Processing node {node_obj.__name__}')
                    result_futures.append(executor.submit(process_and_cache_node, node_obj, **node_inputs))
                logger.debug('Waiting for group of nodes to finish...')
                wait(result_futures)
                # This is not actually a no-op- if any exceptions occur during node processing they get re-raised when
                # calling result()
                [future.result() for future in result_futures]
                logger.debug('Finished processing group of nodes')
        return outputs[self.transformation.__name__].value

    def process_node(self, node_obj, **inputs):
        return node_obj(**inputs)

    def plot(self):
        self.node_graph.plot()


def node(*args, **kwargs):
    """
    Decorator factory that returns the given function wrapped inside a Node class
    """
    def decorator(func):
        return Node(func, *args, **kwargs)
    return decorator
