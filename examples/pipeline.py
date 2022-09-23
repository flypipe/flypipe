import inspect

import networkx as nx
from matplotlib import pyplot as plt

import functools


def node(*args_decorator, **kwargs_decorator):
    def decorator_repeat(func):
        @functools.wraps(func)
        def wrapper_repeat(*args, **kwargs):

            if "graph" in kwargs:
                graph = kwargs["graph"]

                if func.__name__ not in graph:
                    graph.add_node(func.__name__, func=func)

                if "inputs" in kwargs_decorator:
                    for f in kwargs_decorator["inputs"]:
                        if f.__name__ not in graph:
                            graph.add_node(f.__name__, func=f)
                        graph.add_edge(f.__name__, func.__name__)

                        f(graph=graph)

                return graph

            value = func(*args, **kwargs)
            return value

        return wrapper_repeat

    return decorator_repeat


@node(type="pyspark")
def t1():
    return 1


@node(type="pyspark", inputs=[t1])
def t2(t1):
    return t1 + 1


@node(type="pandas", inputs=[t2, t1])
def t3(t2, t1):
    return t2 + t1


@node(type="pandas", inputs=[t3, t1])
def t4(t3, t1):
    return t3 + t1


@node(type="pandas", inputs=[t3])
def t5(t3):
    return t3 + 1


@node(type="pandas", inputs=[t2, t4, t5])
def t6(t2, t4, t5):
    return t2 + t4 + t5


class FlyPipe:
    def run(self, node_parent):

        # Build Graph
        graph = node_parent(graph=nx.DiGraph())

        # Print Graph
        nx.draw(graph, with_labels=True, font_weight="bold")
        plt.show()

        nodes_funcs = nx.get_node_attributes(graph, "func")

        # Run Graph
        outputs = {}
        nodes = [node for node in graph if graph.in_degree(node) == 0]
        while nodes:
            for node in nodes:
                print(f"\nRunning node {node}")
                args = inspect.signature(nodes_funcs[node])
                inputs_ = [outputs[k] for k, v in args.parameters.items()]
                outputs[node] = nodes_funcs[node](*inputs_)

                graph.remove_node(node)
                print(f"Arguments expected for {node}: {args}")
                print(f"Inputs for {node}: {inputs_}")
                print(f"Output of {node}: {outputs[node]}")
            nodes = [node for node in graph if graph.in_degree(node) == 0]

        return outputs[node_parent.__name__]


result = FlyPipe().run(t6)


"""
Running node t1
Arguments expected for t1: ()
Inputs for t1: []
Output of t1: 1

Running node t2
Arguments expected for t2: (t1)
Inputs for t2: [1]
Output of t2: 2

Running node t3
Arguments expected for t3: (t2, t1)
Inputs for t3: [2, 1]
Output of t3: 3

Running node t4
Arguments expected for t4: (t3, t1)
Inputs for t4: [3, 1]
Output of t4: 4

Running node t5
Arguments expected for t5: (t3)
Inputs for t5: [3]
Output of t5: 4

Running node t6
Arguments expected for t6: (t2, t4, t5)
Inputs for t6: [2, 4, 4]
Output of t6: 10

Result t6 by invoking 'run': 10
Result t6 manual inputs as dict: 10
Result t7 declared inputs: 10

Transformations dependencies => DiGraph with 6 nodes and 9 edges
Execution Plan => DiGraph with 6 nodes and 7 edges


"""
