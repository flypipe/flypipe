from flypipe.data_type import String
from flypipe.node import node
from flypipe.schema.column import Column
from flypipe.schema.schema import Schema
from tests.transformations.group_1.t1 import t1
from tests.transformations.group_2.t1 import t1 as t1_group2

@node(
    type="pandas",
    dependencies=[
        t1.select("c1"),
        t1_group2.select("c1").alias("t1_group2")
    ],
    output = Schema([
        Column("c1_group1_t1", String(), 'dummy'),
        Column("c1_group2_t1", String(), 'dummy'),
    ])
)
def t3(t1, t1_group2):
    t1['c1_group1_t1'] = t1['c1']
    t1['c1_group2_t1'] = t1_group2['c1']

    return t1

import os
import inspect
print(os.path.relpath(inspect.getfile(t3.function)))
with open('test.html', 'w') as f:
    html = t3.html(width=-1, height=-1)
    f.writelines(html)

