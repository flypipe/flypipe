import networkx as nx
import logging
from matplotlib import pyplot as plt

from flypipe.data_type import Boolean
from flypipe.node import node
import flypipe.node as node_module

logging.basicConfig(level=logging.DEBUG)


@node(type="spark")
def t1():
    return 1


@node(type="spark", inputs=[t1])
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
def t6(**dfs):
    return dfs["t2"] + dfs["t4"] + dfs["t5"]


@node(type="pandas", inputs=[t2, t4, t5])
def t7(t2, t4, t5):
    return t2 + t4 + t5


result = t6.run()
print(result)
# t6.plot()
