import logging
from concurrent.futures import ThreadPoolExecutor, wait
from flypipe.node_graph import NodeGraph

logger = logging.getLogger(__name__)


class Node:
    def __init__(self, transformation, *args, **kwargs):
        self.transformation = transformation
        self.inputs = kwargs["inputs"] if "inputs" in kwargs else []
        self.node_graph = NodeGraph(self)

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

        def process_node(node_obj, **inputs):
            outputs[node_obj.__name__] = node_obj(**inputs)

        with ThreadPoolExecutor() as executor:
            for node_group in dependency_chain:
                logger.debug(f'Started processing group of nodes {node_group}')
                result_futures = []
                for node_name in node_group:
                    node_obj = self.node_graph.get_node(node_name)
                    try:
                        node_inputs = {input_node.__name__: outputs[input_node.__name__] for input_node in node_obj.inputs}
                    except KeyError as ex:
                        raise ValueError(f'Unable to process transformation {node_obj.__name__}, missing input {ex.args[0]}')

                    logger.debug(f'Processing node {node_obj.__name__}')
                    result_futures.append(executor.submit(process_node, node_obj, **node_inputs))
                logger.debug('Waiting for group of nodes to finish...')
                wait(result_futures)
                logger.debug('Finished processing group of nodes')
        return outputs[self.transformation.__name__]

    def plot(self):
        self.node_graph.plot()


def node(*args, **kwargs):
    """
    Decorator factory that returns the given function wrapped inside a Node class
    """
    def decorator(func):
        return Node(func, *args, **kwargs)
    return decorator
