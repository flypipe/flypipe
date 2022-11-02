from flypipe.data_type import String
from flypipe.node import node
from flypipe.schema import Schema, Column

from tests.transformations.data import t0

@node(
    type='pandas',
    dependencies=[t0.select("c1")],
    output=Schema([
        Column('c1', String(), 'dummy'),
    ])
)
def t1(t0):
    t0['c1'] = t0['c1'] + ' group_2_t1'
    return t0