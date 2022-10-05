from flypipe.data_type import Decimal, Integer

from flypipe.datasource.spark import Spark
from flypipe.node import node
from flypipe.schema.column import Column
from flypipe.schema.schema import Schema

import pandas as pd

from tests.utils.spark import spark

stored_df = spark.createDataFrame(schema=('dummy',), data=[(1,)])

spark.sql("create database if not exists raw")
spark.sql("create view raw.table as select 1 as dummy")


@node(
    type="pyspark",
    dependencies=[
        Spark.table("raw.table").select('dummy')
    ],
    output=Schema([
        Column('dummy', Decimal(10, 2))
    ])
)
def t0(table):
    return table


@node(type='pandas',
      dependencies=[t0],
      output=Schema([Column('dummy', Integer())]))
def t5():
    return pd.DataFrame(data=[{'dummy': [1]}])


@node(type='pandas',
      dependencies=[t0],
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

@node(type='pyspark',
      dependencies=[t6],
      output=Schema([Column('dummy', Integer())]))
def t31(t6):
    return t6

@node(type='pyspark',
      dependencies=[t6],
      output=Schema([Column('dummy', Integer())]))
def t32(t6):
    return t6

@node(type='pyspark',
      dependencies=[t6],
      output=Schema([Column('dummy', Integer())]))
def t33(t6):
    return t6


@node(type='pyspark',
      dependencies=[t6],
      output=Schema([Column('dummy', Integer())]))
def t34(t6):
    return t6

@node(type='pyspark',
      dependencies=[t6],
      output=Schema([Column('dummy', Integer())]))
def t35(t6):
    return t6


@node(type='pandas',
      dependencies=[t2, t3, t7, t31, t32, t33, t34, t35],
      output=Schema([Column('dummy', Integer())]))
def t1(t2, t3, t7, t31, t32, t33, t34, t35):
    return t2

@node(type='pyspark',
      dependencies=[t1, t6],
      tags=['model', 'split'],
      output=Schema([Column('dummy', Integer())]))
def t8(t1, t6):
    return t1

@node(type='pyspark',
      tags=['model', 'scale'],
      dependencies=[t2],
      output=Schema([Column('dummy', Integer())]))
def t9(t2):
    return t2


@node(type='pyspark',
      tags=['model', 'train'],
      dependencies=[t8, t9],
      output=Schema([Column('dummy', Integer())]))
def t10(t8, t9):
    return t9


with open('test.html', 'w') as f:
    html = t10.inputs(t2=pd.DataFrame(data=[{'dummy': [1]}])).html(width=-1, height=-1)
    f.writelines(html)

