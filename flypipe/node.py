import inspect
import networkx as nx
from functools import wraps

from flypipe.node_graph import NodeGraph


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
        for node_group in dependency_chain:
            for node_name in node_group:
                node = self.node_graph.get_node(node_name)
                try:
                    node_inputs = {node.__name__: outputs[node.__name__] for node in node.inputs}
                except KeyError as ex:
                    raise ValueError(f'Unable to process transformation {node.__name__}, missing input {ex.args[0]}')
                outputs[node.__name__] = node(**node_inputs)
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
