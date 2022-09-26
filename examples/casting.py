import datetime
from flypipe.schema.column import Column
from flypipe.schema.schema import Schema
from flypipe.schema.types.date import Date
from flypipe.schema.types.string import String
from flypipe.spark_node.context import get_spark_session
from flypipe.node import node


@node(type='spark', output=Schema([
    Column('name', String()),
    Column('birth_date', Date())
]))
def test1():
    spark = get_spark_session()
    return spark.createDataFrame(schema=('name', 'birth_date'), data=[(6, datetime.date(1994, 8, 22))])


@node(type='pandas', inputs=[test1])
def test2(test1):
    return test1


print(test2.run())
