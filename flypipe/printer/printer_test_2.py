import pandas as pd

from flypipe.node import node


@node(
    type="pandas",
    description="Only outputs a pandas dataframe"
)
def t0():
    return pd.DataFrame(data={"fruit": ["mango", "lemon"]})


@node(
    type="pandas",
    description="Only imports node t0 and outputs whatever t0 is returning",
    dependencies=[t0]
)
def t1(t0):
    return t0


if __name__ == "__main__":
    with open("test.html", "w") as f:
        html = t0.html(width=-1, height=-1)
        f.writelines(html)
