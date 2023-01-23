# pylint: disable=duplicate-code
import pandas as pd
import pyspark.pandas as ps
import pyspark.sql.functions as F
import pytest
from pyspark_test import assert_pyspark_df_equal
from tabulate import tabulate

from flypipe.datasource.spark import Spark
from flypipe.exceptions import DataFrameMissingColumns
from flypipe.node import node
from flypipe.schema import Column
from flypipe.schema import Schema
from flypipe.schema.types import Decimal, Integer, String


@pytest.fixture(scope="function")
def spark():
    from flypipe.tests.spark import spark  # pylint: disable=import-outside-toplevel

    (
        spark.createDataFrame(
            schema=("c1", "c2", "c3"), data=[(1, 2, 3)]
        ).createOrReplaceTempView("dummy_table")
    )

    return spark


class TestPySparkNode:
    """Tests on Nodes with pyspark type"""

    def test_exception_invalid_node_type(self):
        with pytest.raises(ValueError):

            @node(type="anything", output=Schema([Column("balance", Decimal(16, 2))]))
            def dummy():
                pass

    def test_skip_datasource(self, spark, mocker):
        """
        When we provide a dependency input to a node, not only does that node not need to be run but we also expect any
        dependencies of the provided node not to be run.

        When t1 has a dependency to a datasource and it is provided then datasource should not run

        """
        spark_dummy_table = Spark("dummy_table")

        @node(
            type="pyspark",
            dependencies=[spark_dummy_table.select("c1", "c2")],
            output=Schema([Column("c1", Integer()), Column("c2", Integer())]),
        )
        def t1(dummy_table):
            return dummy_table

        func_name = spark_dummy_table.function.__name__
        spy = mocker.spy(spark_dummy_table, "function")
        # Filthy hack to stop the spy removing the __name__ attribute from the function
        spark_dummy_table.function.__name__ = func_name

        df = spark.createDataFrame(schema=("c1", "c2", "c3"), data=[(1, 2, 3)])
        output_df = t1.run(spark, inputs={Spark("dummy_table"): df}, parallel=False)
        spy.assert_not_called()
        assert_pyspark_df_equal(
            output_df,
            spark.createDataFrame(
                schema=(
                    "c1",
                    "c2",
                ),
                data=[
                    (
                        1,
                        2,
                    )
                ],
            ),
            check_dtype=False,
        )

    def test_datasource_basic(self, spark):
        stored_df = spark.createDataFrame(schema=("c1",), data=[(1,)])

        @node(
            type="pyspark",
            dependencies=[Spark("dummy_table").select("c1")],
            output=Schema([Column("c1", Decimal(10, 2))]),
        )
        def t1(dummy_table):
            return dummy_table

        df = t1.run(spark, parallel=False)
        assert_pyspark_df_equal(df, stored_df, check_dtype=False)

    def test_conversion_to_pandas(self, spark):
        @node(
            type="pyspark",
            dependencies=[Spark("dummy_table").select("c1")],
            output=Schema([Column("c1", Decimal(10, 2))]),
        )
        def t1(dummy_table):
            return dummy_table

        @node(
            type="pandas",
            dependencies=[t1.select("c1")],
            output=Schema([Column("c1", Decimal(10, 2))]),
        )
        def t2(t1):
            return t1

        df = t2.run(spark, parallel=False)
        assert isinstance(df, pd.DataFrame)

    def test_conversion_to_pandas_on_spark(self, spark):
        @node(
            type="pyspark",
            dependencies=[Spark("dummy_table").select("c1")],
            output=Schema([Column("c1", Decimal(10, 2))]),
        )
        def t1(dummy_table):
            return dummy_table

        @node(
            type="pandas_on_spark",
            dependencies=[t1.select("c1")],
            output=Schema([Column("c1", Decimal(10, 2))]),
        )
        def t2(t1):
            return t1

        df = t2.run(spark, parallel=False)
        assert isinstance(df, ps.DataFrame)

    def test_datasource_case_sensitive_columns(self, spark):
        """
        Test columns case sensitive

        Pandas dataframe
        import pandas as pd
        df = pd.DataFrame(data={"My_Col": [1]})
        print(df['My_Col']) #<-- passes
        print(df['my_col']) #<-- fails

        Pyspark Dataframe
        df = spark.createDataFrame(schema=('My_Col__x',), data=[(1,)])
        df.select('My_Col__x') #<-- passes
        df.select('my_col__x') #<-- fails


        """

        my_data = spark.createDataFrame(
            schema=(
                "My_Col__x",
                "My_Col__y",
                "My_Col__z",
            ),
            data=[
                (
                    "dummy",
                    "dummy",
                    "dummy",
                )
            ],
        )

        my_data.createOrReplaceTempView("dummy_table__anything_c")

        @node(
            type="pandas_on_spark",
            dependencies=[
                Spark("dummy_table__anything_c").select("Id", "my_col__x", "My_Col__z")
            ],
            output=Schema(
                [
                    Column("my_col", String()),
                ]
            ),
        )
        def my_col(
            test_pyspark_node_dummy_table__anything_c,
        ):  # pylint: disable=invalid-name
            df = test_pyspark_node_dummy_table__anything_c
            df = df.rename(columns={"my_col__x": "my_col"})
            return df

        with pytest.raises(DataFrameMissingColumns) as exc_info:
            my_col.run(spark, parallel=False)

        expected_error_df = pd.DataFrame(
            data={
                "dataframe": ["", "My_Col__x ", "My_Col__z"],
                "selection": ["Id", "my_col__x", "My_Col__z"],
                "error": ["not found", "Did you mean `My_Col__x`?", "found"],
            }
        )

        expected_error_df = expected_error_df.sort_values(
            ["selection", "dataframe"]
        ).reset_index(drop=True)

        assert (
            str(exc_info.value)
            == f"Flypipe: could not find some columns in the dataframe"
            f"\n\nOutput Dataframe columns: ['My_Col__x', 'My_Col__y', 'My_Col__z']"
            f"\nGraph selected columns: ['Id', 'My_Col__z', 'my_col__x']"
            f"\n\n\n{tabulate(expected_error_df, headers='keys', tablefmt='mixed_outline')}\n"
        )

    def test_duplicated_output_columns(self, spark):
        @node(
            type="pandas_on_spark",
            dependencies=[Spark("dummy_table").select("c1", "c2")],
            output=Schema([Column("c1", String()), Column("c2", String())]),
        )
        def t1(dummy_table):
            return dummy_table

        @node(
            type="pandas_on_spark",
            dependencies=[t1.select("c1", "c2")],
            output=Schema([Column("c1", String()), Column("c2", String())]),
        )
        def t2(t1):
            return t1

        @node(
            type="pandas_on_spark",
            dependencies=[t1.select("c1")],
            output=Schema([Column("c1", String())]),
        )
        def t3(t1):
            return t1

        @node(
            type="pyspark",
            dependencies=[t2.select("c1", "c2"), t1.select("c1")],
            output=Schema([Column("c2", String())]),
        )
        def t4(t1, t2):  # pylint: disable=unused-argument
            return t2

        t4._create_graph()  # pylint: disable=protected-access
        for node_name in t4.node_graph.graph:
            n = t4.node_graph.get_node(node_name)
            if n["transformation"].__name__ == "t4":
                continue
            assert len(n["output_columns"]) == len(set(n["output_columns"]))

        df = t4.run(spark, parallel=False)
        assert_pyspark_df_equal(
            df, spark.createDataFrame(schema=("c2",), data=[("2",)]), check_dtype=False
        )

    def test_dataframes_are_isolated_from_nodes(self, spark):
        @node(
            type="pyspark",
            output=Schema([Column("c1", String()), Column("c2", String())]),
        )
        def t1():
            return spark.createDataFrame(pd.DataFrame(data={"c1": ["1"], "c2": ["2"]}))

        @node(
            type="pyspark",
            dependencies=[t1.select("c1", "c2")],
            output=Schema(
                [
                    Column("c1", String()),
                    Column("c2", String()),
                    Column("c3", String()),
                ]
            ),
        )
        def t2(t1):
            t1 = t1.withColumn("c1", F.lit("t2 set this value"))
            t1 = t1.withColumn("c3", F.lit("t2 set this value"))
            return t1

        @node(
            type="pyspark",
            dependencies=[t1.select("c1", "c2"), t2.select("c1", "c2", "c3")],
            output=Schema(
                [
                    Column("c1", String()),
                    Column("c2", String()),
                    Column("c3", String()),
                ]
            ),
        )
        def t3(t1, t2):
            t1 = t1.toPandas()
            t2 = t2.toPandas()
            assert list(t1.columns) == ["c1", "c2"]
            assert t1.loc[0, "c1"] == "1"
            assert t1.loc[0, "c2"] == "2"
            assert list(t2.columns) == ["c1", "c2", "c3"]
            assert t2.loc[0, "c1"] == "t2 set this value"
            assert t2.loc[0, "c2"] == "2"
            assert t2.loc[0, "c3"] == "t2 set this value"
            return spark.createDataFrame(t2)

        t3.run(spark, parallel=False)


# pylint: enable=duplicate-code
