import pandas as pd

from flypipe.node import node
from flypipe.schema import Schema, Column
from flypipe.schema.types import String


@node(
    type="pandas",
    dependencies=[],
    output=Schema(
        [
            Column("c1", String(), "dummy"),
        ]
    ),
)
def t0():
    return pd.DataFrame(data={"c1": ["t0"]})
