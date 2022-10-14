
import pandas as pd
import pytest
from pyspark.sql.types import DecimalType

from flypipe.converter.pyspark_schema_reader import PySparkSchemaReader
from flypipe.data_type import String, Integer, Date, Decimals, Byte, Float, Double, Long, Short, Boolean, Timestamp, \
    Array, Map
from flypipe.schema.column import Column
from flypipe.schema.schema import Schema
from flypipe.utils import DataFrameType


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark

    return spark



@pytest.fixture(scope="function")
def pyspark_df(spark):

    """
    decimal DecimalType(10,0) {}
    float FloatType {}
    double DoubleType {}
    integer IntegerType {}
    long LongType {}
    short ShortType {}
    timestamp TimestampType {}
    string StringType {}
    boolean BooleanType {}
    date DateType {}
    """

    return spark.sql("""
    SELECT
        cast(1 as Byte) as byte,
        cast(1.0 as Decimal(26,3)) as decimal,
        cast(1.0 as float) as float,
        cast(1.0 as double) as double,
        cast(1.0 as int) as int,
        cast(1.0 as long) as long,
        cast(1.0 as short) as short,
        cast(now() as timestamp) as timestamp,
        "1" as string,
        cast(1 as boolean) as boolean,
        cast(now() as date) as date,
        array(array(array(1))) as array_of_integer,
        array(array(array(cast(now() as date)), array(cast(now() as date)))) as array_of_date,        
        map('a',map('b', 1), 1, map('b', 1)) as map_of_map
        --map('a', named_struct('a',map('b', 1))) as map_of_struct
    """)


@pytest.fixture(scope="function")
def pandas_on_spark_df(pyspark_df):
    return pyspark_df.to_pandas_on_spark()


class TestPySparkSchemaReader:

    def test_pyspark(self, spark, pyspark_df):

        schema = PySparkSchemaReader.read(pyspark_df)

        expected_schema = Schema([
                            Column("byte", Byte(), "no description"),
                            Column("decimal", Decimals(precision=26, scale=3), "no description"),
                            Column("float", Float(), "no description"),
                            Column("double", Double(), "no description"),
                            Column("int", Integer(), "no description"),
                            Column("long", Long(), "no description"),
                            Column("short", Short(), "no description"),
                            Column("timestamp", Timestamp(fmt="%Y-%m-%d %H:%M:%S"), "no description"),
                            Column("string", String(), "no description"),
                            Column("boolean", Boolean(), "no description"),
                            Column("date", Date(fmt="%Y-%m-%d"), "no description"),
	                        Column("array_of_integer", Array(Array(Array(Integer()))), "no description"),
                            Column("array_of_date", Array(Array(Array(Date(fmt="%Y-%m-%d")))), "no description"),
                            Column("map_of_map", Map(String(), Map(String(), Integer())), "no description"),
                        ])

        assert str(schema) == str(expected_schema)