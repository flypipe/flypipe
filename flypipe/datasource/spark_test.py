from decimal import Decimal as PythonDecimal

import pytest
from pyspark.sql.types import StructType, StructField, DecimalType
from pyspark_test import assert_pyspark_df_equal

from flypipe import node
from flypipe.datasource.spark import Spark
from flypipe.schema import Schema, Column
from flypipe.schema.types import Decimal


@pytest.fixture(scope="function")
def spark():
    from flypipe.tests.spark import spark

    (
        spark.createDataFrame(
            schema=("c0", "c1"),
            data=[
                (
                    0,
                    1,
                )
            ],
        ).createOrReplaceTempView("dummy_table1")
    )

    (
        spark.createDataFrame(schema=("c2",), data=[(2,)]).createOrReplaceTempView(
            "dummy_table2"
        )
    )

    (
        spark.createDataFrame(schema=("c3",), data=[(3,)]).createOrReplaceTempView(
            "dummy_table3"
        )
    )
    return spark


class TestSparkDataSource:
    """Tests for SparkDataSource"""

    def test_load(self, spark):
        """
        Test basic functionality of the datasource i.e from Spark('dummy_table1').select('c1') we a) pick up the
        contents of dummy_table1 which we earlier prepared in the spark fixture and b) limit the contents to the
        selected c1 column.
        """
        schema = Schema([Column("c1", Decimal(16, 2), "dummy")])

        @node(
            type="pyspark",
            dependencies=[Spark("dummy_table1").select("c1")],
            output=schema,
        )
        def t1(dummy_table1):
            return dummy_table1

        df_expected = spark.createDataFrame(
            schema=StructType([StructField("c1", DecimalType(16, 2))]),
            data=[(PythonDecimal(1),)],
        )
        assert_pyspark_df_equal(df_expected, t1.run(spark, parallel=False))

    def test_multiple_sources(self, spark):
        """
        Verify that everything works ok with multiple datasources.
        """

        @node(
            type="pyspark",
            dependencies=[Spark("dummy_table1").select("c0")],
            output=Schema([Column("c0", Decimal(16, 2), "dummy")]),
        )
        def t0(dummy_table1):
            return dummy_table1

        @node(
            type="pyspark",
            dependencies=[Spark("dummy_table1").select("c1")],
            output=Schema([Column("c1", Decimal(16, 2), "dummy")]),
        )
        def t1(dummy_table1):
            return dummy_table1

        @node(
            type="pyspark",
            dependencies=[Spark("dummy_table2").select("c2")],
            output=Schema([Column("c2", Decimal(16, 2), "dummy")]),
        )
        def t2(dummy_table2):
            return dummy_table2

        @node(
            type="pyspark",
            dependencies=[Spark("dummy_table3").select("c3")],
            output=Schema([Column("c3", Decimal(16, 2), "dummy")]),
        )
        def t3(dummy_table3):
            return dummy_table3

        @node(
            type="pyspark",
            dependencies=[
                t0.select("c0"),
                t1.select("c1"),
                t2.select("c2"),
                t3.select("c3"),
            ],
            output=Schema(
                [
                    Column("c0", Decimal(16, 2), "dummy"),
                    Column("c1", Decimal(16, 2), "dummy"),
                    Column("c2", Decimal(16, 2), "dummy"),
                    Column("c3", Decimal(16, 2), "dummy"),
                ]
            ),
        )
        def t4(t0, t1, t2, t3):
            return t0.join(t1).join(t2).join(t3)

        df_expected = spark.createDataFrame(
            schema=StructType(
                [
                    StructField("c0", DecimalType(16, 2)),
                    StructField("c1", DecimalType(16, 2)),
                    StructField("c2", DecimalType(16, 2)),
                    StructField("c3", DecimalType(16, 2)),
                ]
            ),
            data=[
                (PythonDecimal(0), PythonDecimal(1), PythonDecimal(2), PythonDecimal(3))
            ],
        )
        assert_pyspark_df_equal(df_expected, t4.run(spark, parallel=False))

    def test_datasource_consolidate_columns(self, spark, mocker):
        """
        When multiple nodes use a single table in a datasource then we expect that:
        a) the requested columns consolidate into a single query (i.e one query for col1 and col2 and not a query for
        each column)
        b) the query ONLY includes columns that nodes have requested.

        Therefore in the below scenario where we have a node requesting table.c1 and another node requested table.c2, we
        expect one query to table for both columns and for the other column c3 in the table not to be included.
        """
        spark_dummy_table = Spark("dummy_table1")

        @node(
            type="pyspark",
            dependencies=[spark_dummy_table.select("c0")],
            output=Schema([Column("c0", Decimal(10, 2), "dummy")]),
        )
        def t1(dummy_table1):
            return dummy_table1

        spark_dummy_table_2 = Spark("dummy_table1")

        @node(
            type="pyspark",
            dependencies=[spark_dummy_table_2.select("c1")],
            output=Schema([Column("c1", Decimal(10, 2), "dummy")]),
        )
        def t2(dummy_table1):
            return dummy_table1

        @node(
            type="pyspark",
            dependencies=[t1.select("c0"), t2.select("c1")],
            output=Schema([Column("c0", Decimal(10, 2)), Column("c1", Decimal(10, 2))]),
        )
        def t3(t1, t2):
            return t1.join(t2)

        func_name = spark_dummy_table.function.__name__
        spy = mocker.spy(spark_dummy_table, "function")
        # Filthy hack to stop the spy removing the __name__ attribute from the function
        spark_dummy_table.function.__name__ = func_name

        func_name2 = spark_dummy_table_2.function.__name__
        spy2 = mocker.spy(spark_dummy_table_2, "function")
        # Filthy hack to stop the spy removing the __name__ attribute from the function
        spark_dummy_table_2.function.__name__ = func_name2

        t3.run(spark, parallel=False)
        spy.assert_not_called()
        spy2.assert_called_once()

        assert_pyspark_df_equal(
            spy2.spy_return,
            spark.createDataFrame(schema=("c0", "c1"), data=[(0, 1)]),
            check_dtype=False,
        )
