import os

import pandas as pd

from flypipe import node
from flypipe.cache import Cache
from flypipe.tests.spark import spark


def read():
    print("reading result from test.csv")
    return pd.read_csv("test.csv")


def write(df):
    print("writing results to test.csv")
    df.to_csv("test.csv", index=False)


def exists():
    print("checking cache exists")
    return os.path.exists("test.csv")


cache = Cache(read=read, write=write, exists=exists)


@node(type="pandas")
def t0():
    return pd.DataFrame(data={"t0": [1]})


@node(type="pandas")
def t1():
    return pd.DataFrame(data={"t1": [1]})


@node(type="pandas", dependencies=[t1], cache=cache)
def t4(t1):
    return t1


@node(type="pandas", dependencies=[t0, t1], cache=cache)
def t2(t0, t1):
    return t0


@node(
    type="pandas",
    dependencies=[t2, t4],
)
def t3(t2, t4):
    return t2


df = t3.run(spark)

for node_name in t3.node_graph.graph.nodes:
    print(node_name, t3.node_graph.get_node(node_name))

with open("test.html", "w", encoding="utf-8") as f:
    f.writelines(t3.html(spark, cache={"disable": [t2]}))
