import logging
from concurrent.futures import ThreadPoolExecutor, wait
from flypipe.node_graph import NodeGraph
from collections import namedtuple
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class Node(ABC):

    TYPES = {}
    TYPE = None

    def __init__(self, transformation, inputs=None):
        self.transformation = transformation
        self.inputs = inputs or []
        self.node_graph = NodeGraph(self)

    @classmethod
    def get_class(cls, node_type):
        try:
            return cls.TYPES[node_type]
        except KeyError:
            raise ValueError(f'Invalid node type {node_type} specified, provide type must be one of {cls.TYPES.keys()}')

    @classmethod
    def register_node_type(cls, node_type_class):
        cls.TYPES[node_type_class.TYPE] = node_type_class

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

    @classmethod
    @abstractmethod
    def convert_dataframe(cls, df, destination_type):
        # TODO- is there a better place to put this? It seems a little out of place here
        pass

    def run(self):
        outputs = {}
        dependency_chain = self.node_graph.get_dependency_chain()

        def process_and_cache_node(node_obj, **inputs):
            outputs[node_obj.__name__] = self.process_node(node_obj, **inputs)

        with ThreadPoolExecutor() as executor:
            for node_group in dependency_chain:
                logger.debug(f'Started processing group of nodes {node_group}')
                result_futures = []
                for node_name in node_group:
                    node_obj = self.node_graph.get_node(node_name)
                    try:
                        node_inputs = {}
                        for input_node in node_obj.inputs:
                            # We need to ensure that the result of each input is converted to the same type as the node
                            # that's processing it.
                            input_value = input_node.convert_dataframe(outputs[input_node.__name__], node_obj.TYPE)
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
        return outputs[self.transformation.__name__]

    def process_node(self, node_obj, **inputs):
        return node_obj(**inputs)

    def plot(self):
        self.node_graph.plot()


def node(*args, **kwargs):
    """
    Decorator factory that returns the given function wrapped inside a Node class
    """
    def decorator(func):
        try:
            node_type = kwargs.pop('type')
        except KeyError:
            node_type = None
        return Node.get_class(node_type)(func, *args, **kwargs)
    return decorator
