import inspect
import networkx as nx
from matplotlib import pyplot as plt


class node:
    def __init__(self, *args, **kwargs):
        self.inputs = kwargs["inputs"] if "inputs" in kwargs else []

    @property
    def name(self):
        return self.function.__name__

    def __call__(self, *args_function, **kwargs_function):
        if (
            isinstance(args_function, tuple)
            and len(args_function) == 1
            and inspect.isfunction(args_function[0])
        ):
            # mapping the function
            self.function = args_function[0]
            return self
        else:
            # calling the function by providing manually all inputs
            return self.function(*args_function, **kwargs_function)

    def get_graph(self, execution=True):

        graph = nx.DiGraph()
        graph.add_node(
            self.name, function=self.function, inputs=[i.name for i in self.inputs]
        )

        if self.inputs:
            for input in self.inputs:

                graph.add_node(input.name, function=input.function)
                graph.add_edge(input.name, self.name)
                graph = nx.compose(graph, input.get_graph(execution=execution))

        return graph

    def run(self):

        # Build Graph
        graph = self.get_graph()
        nodes_funcs = nx.get_node_attributes(graph, "function")
        nodes_inputs = nx.get_node_attributes(graph, "inputs")

        # Run Graph
        outputs = {}
        nodes = [node for node in graph if graph.in_degree(node) == 0]
        while nodes:
            for node in nodes:
                print(f"\nRunning node {node}")

                inputs_ = {input: outputs[input] for input in nodes_inputs[node]}
                if inputs_:
                    outputs[node] = nodes_funcs[node](**inputs_)
                else:
                    outputs[node] = nodes_funcs[node]()

                graph.remove_node(node)
                print(f"Arguments expected for {node}: {inputs_}")
                print(f"Inputs for {node}: {inputs_}")
                print(f"Output of {node}: {outputs[node]}")
            nodes = [node for node in graph if graph.in_degree(node) == 0]

        return outputs[self.name]


@node(mode="pyspark")
def t1():
    return 1


@node(mode="pyspark", inputs=[t1])
def t2(t1):
    return t1 + 1


@node(mode="pandas", inputs=[t2, t1])
def t3(t2, t1):
    return t2 + t1


@node(mode="pandas", inputs=[t3, t1])
def t4(t3, t1):
    return t3 + t1


@node(mode="pandas", inputs=[t3])
def t5(t3):
    return t3 + 1


@node(mode="pandas", inputs=[t2, t4, t5])
def t6(**dfs):
    return dfs["t2"] + dfs["t4"] + dfs["t5"]


@node(mode="pandas", inputs=[t2, t4, t5])
def t7(t2, t4, t5):
    return t2 + t4 + t5


result = t6.run()
print()
print("Result t6 by invoking 'run':", result)
print("Result t6 manual inputs as dict:", t6(**{"t2": 2, "t4": 4, "t5": 4}))
print("Result t7 declared inputs:", t7(2, 4, 4))

print()
plt.title("Transformations dependencies")
g = t6.get_graph(execution=False)
print("Transformations dependencies =>", g)
nx.draw(g, with_labels=True)

plt.figure()
plt.title("Execution Plan")
g = t6.get_graph(execution=True)
print("Execution Plan =>", g)
nx.draw(g, with_labels=True)
plt.show()


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
"""
