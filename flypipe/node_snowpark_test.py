import os

import pandas as pd
import pytest
from snowflake.snowpark import Session as SnowflakeSession

from flypipe.node import node
from flypipe.schema import Schema, Column
from flypipe.schema.types import String, Decimal
from flypipe.utils import DataFrameType, dataframe_type


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") != "SNOWFLAKE",
    reason="Snowpark tests require RUN_MODE=SNOWFLAKE",
)
class TestNodeSnowpark:
    """Unit tests on the Node class - Snowpark"""

    def test_run_dataframe_conversion(self, snowflake_session):
        """
        If a node is dependant upon a node of a different dataframe type, then we expect the output of the parent node
        to be converted when it's provided to the child node.
        """

        @node(type="snowpark", output=Schema([Column("C1", Decimal(10, 2))]))
        def t1():
            return snowflake_session.create_dataframe(
                pd.DataFrame(data={"C1": [1], "C2": [2], "C3": [3]})
            )

        @node(type="pandas", dependencies=[t1.select("C1")])
        def t2(t1):
            return t1

        t1_output = t1.run(snowflake_session)
        t2_output = t2.run(snowflake_session)
        assert dataframe_type(t1_output) == DataFrameType.SNOWPARK
        assert isinstance(t2_output, pd.DataFrame)

    def test_adhoc_call(self, snowflake_session):
        """
        If we call a node directly with a function call we should skip calling the input dependencies and instead use
        the passed in arguments
        """

        @node(
            type="snowpark",
            output=Schema(
                [
                    Column("C1", Decimal(16, 2), "dummy"),
                    Column("C2", Decimal(16, 2), "dummy"),
                ]
            ),
        )
        def t1():
            raise Exception("I shouldnt be run!")

        @node(
            type="snowpark",
            dependencies=[t1.select("C1")],
            output=Schema([Column("C1", Decimal(16, 2), "dummy")]),
        )
        def t2(t1):
            from snowflake.snowpark.functions import col

            return t1.with_column("C1", col("C1") + 1)

        df = snowflake_session.create_dataframe(
            pd.DataFrame({"C1": [1]}), schema=["C1"]
        )
        expected_df = snowflake_session.create_dataframe(
            pd.DataFrame({"C1": [2]}), schema=["C1"]
        )

        result = t2(df)
        result_collected = sorted(result.collect(), key=lambda row: row.C1)
        expected_collected = sorted(expected_df.collect(), key=lambda row: row.C1)

        assert len(result_collected) == len(expected_collected)
        assert result_collected[0].C1 == expected_collected[0].C1

    def test_run_isolated_dependencies_snowpark(self, snowflake_session):
        """
        When we pass a dataframe dependency from an ancestor node to a child node the dataframe should be completely
        isolated. That is, any changes to the input dataframe should not modify the output of the parent node.
        """

        @node(
            type="snowpark",
        )
        def t1():
            return snowflake_session.create_dataframe(
                pd.DataFrame({"C1": [1]}), schema=["C1"]
            )

        @node(type="snowpark", dependencies=[t1])
        def t2(t1):
            from snowflake.snowpark.functions import col

            t1 = t1.with_column("C1", col("C1") + 1)
            return t1

        @node(type="snowpark", dependencies=[t1, t2])
        def t3(t1, t2):
            # t1 returns 1, t2 adds 1 to t1 (returning 2), t3 adds t1 and t2. t2 adding 1 to t1 should not modify the
            # output of t1
            assert t1.collect()[0].C1 == 1
            assert t2.collect()[0].C1 == 2
            return t1

        t3.run()

    def test_function_argument_signature(self, snowflake_session):
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

        @node(type="snowpark")
        def t1():
            return snowflake_session.create_dataframe(
                pd.DataFrame({"C1": [1], "C2": [2]}), schema=["C1", "C2"]
            )

        @node(
            type="snowpark",
            dependencies=[t1.select("C1")],
            requested_columns=True,
            session_context=True,
        )
        def t2(t1, requested_columns, session):
            assert requested_columns == ["C1"]
            assert dataframe_type(t1) == DataFrameType.SNOWPARK
            assert t1.columns == ["C1"]
            assert isinstance(session, SnowflakeSession)
            return t1

        @node(
            type="snowpark",
            dependencies=[
                t1.select("C1", "C2"),
                t2.select("C1"),
            ],
            session_context=True,
        )
        def t3(t2, t1, session):
            assert dataframe_type(t1) == DataFrameType.SNOWPARK
            assert t1.columns == ["C1", "C2"]

            assert dataframe_type(t2) == DataFrameType.SNOWPARK
            assert t2.columns == ["C1"]

            assert isinstance(session, SnowflakeSession)

            return t1

        t3.run(snowflake_session)

    def test_conversion_to_pandas(self, snowflake_session):
        """Test conversion from Snowpark to Pandas"""

        @node(
            type="snowpark",
            output=Schema([Column("C1", Decimal(10, 2))]),
        )
        def t1():
            return snowflake_session.create_dataframe(pd.DataFrame({"C1": [1]}))

        @node(
            type="pandas",
            dependencies=[t1.select("C1")],
            output=Schema([Column("C1", Decimal(10, 2))]),
        )
        def t2(t1):
            return t1

        df = t2.run(snowflake_session)
        assert isinstance(df, pd.DataFrame)

    def test_duplicated_output_columns(self, snowflake_session):
        """Test handling of duplicated output columns"""

        @node(
            type="snowpark",
            output=Schema([Column("C1", String()), Column("C2", String())]),
        )
        def t1():
            return snowflake_session.create_dataframe(
                pd.DataFrame({"C1": ["1"], "C2": ["2"], "C3": ["3"]})
            )

        @node(
            type="snowpark",
            dependencies=[t1.select("C1", "C2")],
            output=Schema([Column("C1", String()), Column("C2", String())]),
        )
        def t2(t1):
            return t1

        @node(
            type="snowpark",
            dependencies=[t1.select("C1")],
            output=Schema([Column("C1", String())]),
        )
        def t3(t1):
            return t1

        @node(
            type="snowpark",
            dependencies=[t2.select("C1", "C2"), t1.select("C1")],
            output=Schema([Column("C2", String())]),
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

        df = t4.run(snowflake_session)
        expected_df = snowflake_session.create_dataframe(pd.DataFrame({"C2": ["2"]}))

        # Compare DataFrames
        result_collected = sorted(df.collect(), key=lambda row: row.C2)
        expected_collected = sorted(expected_df.collect(), key=lambda row: row.C2)

        assert len(result_collected) == len(expected_collected)
        assert result_collected[0].C2 == expected_collected[0].C2

    def test_dataframes_are_isolated_from_nodes(self, snowflake_session):
        """Test that dataframes are isolated between nodes"""
        from snowflake.snowpark.functions import lit

        @node(
            type="snowpark",
            output=Schema([Column("C1", String()), Column("C2", String())]),
        )
        def t1():
            return snowflake_session.create_dataframe(
                pd.DataFrame(data={"C1": ["1"], "C2": ["2"]})
            )

        @node(
            type="snowpark",
            dependencies=[t1.select("C1", "C2")],
            output=Schema(
                [
                    Column("C1", String()),
                    Column("C2", String()),
                    Column("C3", String()),
                ]
            ),
        )
        def t2(t1):
            t1 = t1.with_column("C1", lit("t2 set this value"))
            t1 = t1.with_column("C3", lit("t2 set this value"))
            return t1

        @node(
            type="snowpark",
            dependencies=[t1.select("C1", "C2"), t2.select("C1", "C2", "C3")],
            output=Schema(
                [
                    Column("C1", String()),
                    Column("C2", String()),
                    Column("C3", String()),
                ]
            ),
        )
        def t3(t1, t2):
            t1_pd = t1.to_pandas()
            t2_pd = t2.to_pandas()
            assert list(t1_pd.columns) == ["C1", "C2"]
            assert t1_pd.loc[0, "C1"] == "1"
            assert t1_pd.loc[0, "C2"] == "2"
            assert list(t2_pd.columns) == ["C1", "C2", "C3"]
            assert t2_pd.loc[0, "C1"] == "t2 set this value"
            assert t2_pd.loc[0, "C2"] == "2"
            assert t2_pd.loc[0, "C3"] == "t2 set this value"
            return snowflake_session.create_dataframe(t2_pd)

        t3.run(snowflake_session)
