import os
from unittest import mock

import pandas as pd
import pyspark
import pyspark.pandas as ps
import pyspark.sql.functions as F
import pytest
from pyspark.sql import DataFrame
from pyspark.sql.connect.dataframe import DataFrame as SqlConnectDataFrame
from flypipe.tests.pyspark_test import assert_pyspark_df_equal

from flypipe.datasource.spark import Spark
from flypipe.node import node
from flypipe.run_context import RunContext
from flypipe.schema import Schema, Column
from flypipe.schema.types import String, Decimal, Integer
from flypipe.utils import DataFrameType, dataframe_type


@pytest.fixture(scope="function")
def spark_view(spark):

    spark.createDataFrame(
        schema=("c0", "c1"),
        data=[
            (
                0,
                1,
            )
        ],
    ).createOrReplaceTempView("dummy_table1")

    return spark


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") not in ["SPARK", "SPARK_CONNECT"],
    reason="PySpark tests require RUN_MODE=SPARK or SPARK_CONNECT",
)
class TestNodePySpark:
    """Unit tests on the Node class - PySpark"""

    @pytest.mark.parametrize(
        "extra_run_config,expected_df_type",
        [
            ({}, DataFrameType.PANDAS_ON_SPARK),
            ({"pandas_on_spark_use_pandas": False}, DataFrameType.PANDAS_ON_SPARK),
            ({"pandas_on_spark_use_pandas": True}, DataFrameType.PANDAS),
        ],
    )
    def test_input_dataframes_type(
        self, spark_view, mocker, extra_run_config, expected_df_type
    ):
        stub = mocker.stub()

        @node(type="pandas")
        def t1():
            return pd.DataFrame(
                {"fruit": ["Banana", "Apple"], "color": ["Yellow", "Red"]}
            )

        @node(type="pyspark")
        def t2():
            return spark_view.createDataFrame(
                schema=("name", "fruit"), data=[("Chris", "Banana")]
            )

        @node(
            type="pandas_on_spark",
            dependencies=[
                t1.select("fruit", "color"),
                t2.select("name", "fruit"),
            ],
        )
        def t3(t1, t2):
            stub(t1, t2)
            return t1.merge(t2)

        t3.run(spark_view, **extra_run_config)
        assert dataframe_type(stub.call_args[0][0]) == expected_df_type
        assert dataframe_type(stub.call_args[0][1]) == expected_df_type

    def test_run_dataframe_conversion(self, spark_view):
        """
        If a node is dependant upon a node of a different dataframe type, then we expect the output of the parent node
        to be converted when it's provided to the child node.
        """

        @node(type="pandas_on_spark", output=Schema([Column("c1", Decimal(10, 2))]))
        def t1():
            return spark_view.createDataFrame(
                pd.DataFrame(data={"c1": [1], "c2": [2], "c3": [3]})
            ).pandas_api()

        @node(type="pandas", dependencies=[t1.select("c1")])
        def t2(t1):
            return t1

        t1_output = t1.run(spark_view)
        t2_output = t2.run(spark_view)
        assert isinstance(t1_output, ps.frame.DataFrame)
        assert isinstance(t2_output, pd.DataFrame)

    def test_adhoc_call(self, spark_view):
        """
        If we call a node directly with a function call we should skip calling the input dependencies and instead use
        the passed in arguments
        """

        @node(
            type="pyspark",
            dependencies=[Spark("dummy_table").select("c1")],
            output=Schema(
                [
                    Column("c1", Decimal(16, 2), "dummy"),
                    Column("c2", Decimal(16, 2), "dummy"),
                ]
            ),
        )
        def t1(dummy_table):
            raise Exception("I shouldnt be run!")

        @node(
            type="pyspark",
            dependencies=[t1.select("c1")],
            output=Schema([Column("c1", Decimal(16, 2), "dummy")]),
        )
        def t2(t1):
            return t1.withColumn("c1", t1.c1 + 1)

        df = spark_view.createDataFrame(schema=("c1",), data=[(1,)])
        expected_df = spark_view.createDataFrame(schema=("c1",), data=[(2,)])

        assert_pyspark_df_equal(t2(df), expected_df)

    def test_run_isolated_dependencies_pandas_on_spark(self, spark_view):
        """
        When we pass a dataframe dependency from an ancestor node to a child node the dataframe should be completely
        isolated. That is, any changes to the input dataframe should not modify the output of the parent node.
        """

        @node(type="pandas_on_spark")
        def t1():
            return spark_view.createDataFrame(data=[{"c1": 1}]).pandas_api()

        @node(type="pandas_on_spark", dependencies=[t1])
        def t2(t1):
            t1["c1"] += 1
            return t1

        @node(type="pandas_on_spark", dependencies=[t1, t2])
        def t3(t1, t2):
            # t1 returns 1, t2 adds 1 to t1 (returning 2), t3 adds t1 and t2. t2 adding 1 to t1 should not modify the
            # output of t1
            assert t1.loc[0, "c1"] == 1
            assert t2.loc[0, "c1"] == 2
            return t1

        t3.run()

    def test_run_isolated_dependencies_spark(self, spark_view):
        """
        When we pass a dataframe dependency from an ancestor node to a child node the dataframe should be completely
        isolated. That is, any changes to the input dataframe should not modify the output of the parent node.
        """

        @node(
            type="pyspark",
        )
        def t1():
            return spark_view.createDataFrame(data=[{"c1": 1}])

        @node(type="pyspark", dependencies=[t1])
        def t2(t1):
            t1 = t1.withColumn("c1", F.col("c1") + 1)
            return t1

        @node(type="pyspark", dependencies=[t1, t2])
        def t3(t1, t2):
            # t1 returns 1, t2 adds 1 to t1 (returning 2), t3 adds t1 and t2. t2 adding 1 to t1 should not modify the
            # output of t1
            assert t1.collect()[0].c1 == 1
            assert t2.collect()[0].c1 == 2
            return t1

        t3.run()

    def test_pandas_on_spark_use_pandas(self, spark_view):
        """
        When running a graph with pandas_on_spark_use_pandas=True, all pandas_on_spark nodes types should be of type
        pandas. If running straight after, but with pandas_on_spark_use_pandas=False, all pandas_on_spark nodes types
        should be of type pandas_on_spark.
        """

        @node(type="pandas_on_spark", dependencies=[Spark("dummy_table1").select("c1")])
        def t1(dummy_table1):
            return dummy_table1

        t1.run(spark_view, pandas_on_spark_use_pandas=True)
        assert t1.dataframe_type == DataFrameType.PANDAS_ON_SPARK

        t1.run(spark_view, pandas_on_spark_use_pandas=False)
        assert t1.dataframe_type == DataFrameType.PANDAS_ON_SPARK

        # TODO- we should refactor to avoid having to call this private method to build a graph
        t1.create_graph(RunContext(pandas_on_spark_use_pandas=True))
        for n in t1.node_graph.graph.nodes:
            if t1.node_graph.graph.nodes[n]["transformation"].__name__ == "t1":
                assert (
                    t1.node_graph.graph.nodes[n]["transformation"].dataframe_type
                    == DataFrameType.PANDAS
                )

    def test_function_argument_signature(self, spark_view):
        """
        Independent of the order of the argument that the user types for a function,
        it should be given accordingly to what has been specified in the function

        Example:

            @node(...)
            def t(arg3, arg1, arg1):
                ...

            Should be the same as

            @node(...)
            def t(arg1, arg2, arg3):
                ...
        """

        @node(type="pyspark")
        def t1():
            return spark_view.createDataFrame(data=[{"c1": 1, "c2": 2}])

        @node(
            type="pyspark",
            dependencies=[t1.select("c1")],
            requested_columns=True,
            session_context=True,
        )
        def t2(t1, requested_columns, session):
            assert requested_columns == ["c1"]
            assert dataframe_type(t1) == DataFrameType.PYSPARK
            assert t1.columns == ["c1"]
            assert isinstance(session, pyspark.sql.session.SparkSession) or isinstance(
                session, pyspark.sql.connect.session.SparkSession
            )
            return t1

        @node(
            type="pyspark",
            dependencies=[
                t1.select("c1", "c2"),
                t2.select("c1"),
            ],
            session_context=True,
        )
        def t3(t2, t1, session):
            assert dataframe_type(t1) == DataFrameType.PYSPARK
            assert t1.columns == ["c1", "c2"]

            assert dataframe_type(t2) == DataFrameType.PYSPARK
            assert t2.columns == ["c1"]

            assert isinstance(session, pyspark.sql.session.SparkSession) or isinstance(
                session, pyspark.sql.connect.session.SparkSession
            )

            return t1

        t3.run(spark_view)

    def test_spark_sql_conversion(self, spark_view):
        """
        If we use a spark_view sql code then:
        - any dependencies should be saved into a unique table.
        - the dataframes passed into the spark_view sql function should be replaced with the names of the tables they were
        saved into.
        """

        @node(type="pandas")
        def t1():
            return pd.DataFrame({"number": [1]})

        @node(type="spark_sql", dependencies=[t1])
        def t2(t1):
            return f"select number+1 from {t1}"

        dataframe_to_mock = (
            SqlConnectDataFrame
            if os.environ.get("RUN_MODE") == "SPARK_CONNECT"
            else DataFrame
        )
        with mock.patch.object(
            dataframe_to_mock, "createOrReplaceTempView"
        ) as createOrReplaceTempView_mock, mock.patch.object(
            spark_view,
            "sql",
            return_value=spark_view.createDataFrame(schema=("number",), data=[(2,)]),
        ) as sql_mock:
            t2.run(session=spark_view)

        assert sql_mock.call_args[0][0] == "select number+1 from t2__t1"
        assert createOrReplaceTempView_mock.call_args[0][0] == "t2__t1"

    def test_spark_sql_select_column(self, spark_view):
        """
        Basic test for spark_view sql where the dependency has a select column in it.
        There was a bug (DATA-3936) where this use case stacktraces.
        """

        @node(type="pandas")
        def t1():
            return pd.DataFrame({"c1": ["chris"]})

        @node(type="spark_sql", dependencies=[t1.select("c1")])
        def t2(t1):
            return f"select * from {t1}"

        dataframe_to_mock = (
            SqlConnectDataFrame
            if os.environ.get("RUN_MODE") == "SPARK_CONNECT"
            else DataFrame
        )
        with mock.patch.object(
            dataframe_to_mock, "createOrReplaceTempView"
        ) as createOrReplaceTempView_mock, mock.patch.object(
            spark_view,
            "sql",
            return_value=spark_view.createDataFrame(data=[{"number": 2}]),
        ) as sql_mock:
            t2.run(session=spark_view)
        assert sql_mock.call_args[0][0] == "select * from t2__t1"
        assert createOrReplaceTempView_mock.call_args[0][0] == "t2__t1"

    def test_skip_datasource(self, spark_view, mocker):
        """
        When we provide a dependency input to a node, not only does that node not need to be run but we also expect any
        dependencies of the provided node not to be run.

        When t1 has a dependency to a datasource and it is provided then datasource should not run
        """
        from flypipe.datasource.spark import Spark

        spark_dummy_table = Spark("dummy_table")

        spark_view.createDataFrame(
            schema=("c1", "c2", "c3"), data=[(1, 2, 3)]
        ).createOrReplaceTempView("dummy_table")

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

        df = spark_view.createDataFrame(schema=("c1", "c2", "c3"), data=[(1, 2, 3)])
        output_df = t1.run(spark_view, inputs={Spark("dummy_table"): df})
        spy.assert_not_called()
        from flypipe.tests.pyspark_test import assert_pyspark_df_equal

        assert_pyspark_df_equal(
            output_df,
            spark_view.createDataFrame(
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

    def test_datasource_basic(self, spark_view):
        """Test basic datasource functionality with Spark"""
        from flypipe.datasource.spark import Spark

        spark_view.createDataFrame(
            schema=("c1", "c2", "c3"), data=[(1, 2, 3)]
        ).createOrReplaceTempView("dummy_table")

        stored_df = spark_view.createDataFrame(schema=("c1",), data=[(1,)])

        @node(
            type="pyspark",
            dependencies=[Spark("dummy_table").select("c1")],
            output=Schema([Column("c1", Decimal(10, 2))]),
        )
        def t1(dummy_table):
            return dummy_table

        df = t1.run(spark_view)
        from flypipe.tests.pyspark_test import assert_pyspark_df_equal

        assert_pyspark_df_equal(df, stored_df, check_dtype=False)

    def test_conversion_to_pandas(self, spark_view):
        """Test conversion from PySpark to Pandas"""
        from flypipe.datasource.spark import Spark

        spark_view.createDataFrame(
            schema=("c1", "c2", "c3"), data=[(1, 2, 3)]
        ).createOrReplaceTempView("dummy_table")

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

        df = t2.run(spark_view)
        assert isinstance(df, pd.DataFrame)

    def test_conversion_to_pandas_on_spark(self, spark_view):
        """Test conversion from PySpark to Pandas-on-Spark"""
        from flypipe.datasource.spark import Spark

        spark_view.createDataFrame(
            schema=("c1", "c2", "c3"), data=[(1, 2, 3)]
        ).createOrReplaceTempView("dummy_table")

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

        df = t2.run(spark_view)
        assert isinstance(df, ps.DataFrame)

    def test_datasource_case_sensitive_columns(self, spark_view):
        """
        Test columns case sensitive

        Pandas dataframe
        import pandas as pd
        df = pd.DataFrame(data={"My_Col": [1]})
        print(df['My_Col']) #<-- passes
        print(df['my_col']) #<-- fails

        Pyspark Dataframe
        df = spark_view.createDataFrame(schema=('My_Col__x',), data=[(1,)])
        df.select('My_Col__x') #<-- passes
        df.select('my_col__x') #<-- fails
        """
        from flypipe.datasource.spark import Spark
        from flypipe.exceptions import DataFrameMissingColumns
        from tabulate import tabulate

        my_data = spark_view.createDataFrame(
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
        ):
            df = test_pyspark_node_dummy_table__anything_c
            df = df.rename(columns={"my_col__x": "my_col"})
            return df

        # Test that the exception is raised with the correct error message
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

        expected_error_message = (
            f"Flypipe: could not find some columns in the dataframe"
            f"\n\nOutput Dataframe columns: ['My_Col__x', 'My_Col__y', 'My_Col__z']"
            f"\nGraph selected columns: ['Id', 'My_Col__z', 'my_col__x']"
            f"\n\n\n{tabulate(expected_error_df, headers='keys', tablefmt='mixed_outline')}\n"
        )

        # Verify the exception is raised with the correct message
        with pytest.raises(DataFrameMissingColumns) as exc_info:
            my_col.run(spark_view)

        assert str(exc_info.value) == expected_error_message

    def test_duplicated_output_columns(self, spark_view):
        """Test handling of duplicated output columns"""
        from flypipe.datasource.spark import Spark

        spark_view.createDataFrame(
            schema=("c1", "c2", "c3"), data=[(1, 2, 3)]
        ).createOrReplaceTempView("dummy_table")

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
        def t4(t1, t2):
            return t2

        from flypipe.run_context import RunContext

        t4.create_graph(run_context=RunContext())
        for node_name in t4.node_graph.graph:
            n = t4.node_graph.get_node(node_name)
            if n["transformation"].__name__ == "t4":
                continue
            assert len(n["output_columns"]) == len(set(n["output_columns"]))

        df = t4.run(spark_view)
        from flypipe.tests.pyspark_test import assert_pyspark_df_equal

        assert_pyspark_df_equal(
            df,
            spark_view.createDataFrame(schema=("c2",), data=[("2",)]),
            check_dtype=False,
        )

    def test_dataframes_are_isolated_from_nodes(self, spark_view):
        """Test that dataframes are isolated between nodes"""
        @node(
            type="pyspark",
            output=Schema([Column("c1", String()), Column("c2", String())]),
        )
        def t1():
            return spark_view.createDataFrame(
                pd.DataFrame(data={"c1": ["1"], "c2": ["2"]})
            )

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
            return spark_view.createDataFrame(t2)

        t3.run(spark_view)

