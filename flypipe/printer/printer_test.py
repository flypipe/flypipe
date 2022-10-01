import pytest
from pytest_mock import mocker

from flypipe.data_type import Decimal, Integer
from pyspark_test import assert_pyspark_df_equal

from flypipe.datasource.spark import Spark
from flypipe.graph_html import GraphHTML
from flypipe.node import node
from flypipe.schema.column import Column
from flypipe.schema.schema import Schema

import pandas as pd

@node(type='pandas',
      output=Schema([Column('dummy', Integer())]))
def t5():
    return pd.DataFrame(data=[{'dummy': [1]}])


@node(type='pandas',
      output=Schema([Column('dummy', Integer())]))
def t6():
    return pd.DataFrame(data=[{'dummy': [1]}])

@node(type='pandas',
      dependencies=[t6],
      output=Schema([Column('dummy', Integer())]))
def t7(t6):
    return t6

@node(type='pandas_on_spark',
      dependencies=[t5],
      output=Schema([Column('dummy', Integer())]))
def t4(t5):
    return t5

@node(type='pandas',
      dependencies=[t4, t5],
      output=Schema([Column('dummy', Integer())]))
def t2(t4, t5):
    return t5


@node(type='pyspark',
      dependencies=[t6],
      output=Schema([Column('dummy', Integer())]))
def t3(t6):
    return t6


@node(type='pandas',
      dependencies=[t2, t3, t7],
      output=Schema([Column('dummy', Integer())]))
def t1(t2, t3, t7):
    return t2

@node(type='pyspark',
      dependencies=[t1, t6],
      output=Schema([Column('dummy', Integer())]))
def t8(t1, t6):
    return t1

@node(type='pyspark',
      dependencies=[t2],
      output=Schema([Column('dummy', Integer())]))
def t9(t2):
    return t2


@node(type='pyspark',
      dependencies=[t8, t9],
      output=Schema([Column('dummy', Integer())]))
def t10(t8, t9):
    return t9


with open('test.html', 'w') as f:
    f.writelines(t10.inputs(t2=pd.DataFrame(data=[{'dummy': [1]}])).html())

