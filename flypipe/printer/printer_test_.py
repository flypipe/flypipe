from flypipe.datasource.spark import Spark
from flypipe.node import node
from flypipe.schema.column import Column
from flypipe.schema.schema import Schema
from flypipe.schema.types import Decimal
from tests.utils.spark import spark

spark.sql("create database if not exists raw")
spark.sql("create view raw.table1 as select 1 as col1, 2 as col2, 3 as col3, 4 as col4, 5 as col5")

@node(
    type="pyspark",
    dependencies=[
        Spark("raw.table1").select('col1', 'col2')
    ],
    output=Schema([
        Column('col1', Decimal(10, 2), 'dummy'),
        Column('col2', Decimal(10, 2), 'dummy'),
    ]))
def t1(table):
    return table

@node(
    type="pyspark",
    dependencies=[
        Spark("raw.table1").select('col2', 'col3')
    ],
    output=Schema([
        Column('col2', Decimal(10, 2), 'dummy'),
        Column('col3', Decimal(10, 2), 'dummy'),
    ]))
def t2(table):
    return table

@node(
    type="pyspark",
    dependencies=[
        Spark("raw.table1").select('col5')
    ],
    output=Schema([
        Column('col5', Decimal(10, 2), 'dummy'),
    ]))
def t3(table):
    return table


@node(
    type="pyspark",
    description='This is a test',
    dependencies=[
        t1.select('col1'),
        t2.select('col2', 'col3'),
        t3.select('col5')

    ],
    output=Schema([
        Column('col5', Decimal(10, 2), 'dummy'),
    ]))
def t4(t1, t2, t3):
    return t3


with open('test.html', 'w') as f:
    html = t4.html(width=-1, height=-1, skipped_nodes=[Spark("raw.table1")])
    f.writelines(html)

