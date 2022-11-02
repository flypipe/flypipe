import pandas as pd

from flypipe.data_type import String
from flypipe.node import node
from flypipe.schema import Schema, Column



@node(
    type='pandas',
    dependencies=[],
    output=Schema([
        Column('c1', String(), 'dummy'),
    ])
)
def t0():
    return pd.DataFrame(data={
        'c1': ['t0']
    })