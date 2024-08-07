from unittest import mock

import pandas as pd
import pyspark
import pyspark.pandas as ps
import pyspark.sql.functions as F
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql import DataFrame
from pyspark_test import assert_pyspark_df_equal

from flypipe.cache.cache import Cache
from flypipe.config import config_context
from flypipe.converter.dataframe import DataFrameConverter
from flypipe.datasource.spark import Spark
from flypipe.exceptions import DataFrameMissingColumns
from flypipe.node import node, Node
from flypipe.node_function import node_function
from flypipe.run_context import RunContext
from flypipe.schema import Schema, Column
from flypipe.schema.types import String, Decimal, Integer
from flypipe.utils import DataFrameType, dataframe_type


@pytest.fixture(scope="function")
def spark():
    from flypipe.tests.spark import spark

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


class TestNode:
    """Unit tests on the Node class"""

    def test_invalid_type(self):
        """Building a node with a type not in Node.ALLOWED_TYPES should raise an exception"""
        with pytest.raises(ValueError):

            @node(type="dummy")
            def t1():
                return pd.DataFrame()

    @pytest.mark.parametrize(
        "node_type,expected_class",
        [
            ("pyspark", Node),
            ("pandas", Node),
            ("pandas_on_spark", Node),
        ],
    )
    def test_get_class(self, node_type, expected_class):
        assert isinstance(Node(None, node_type), expected_class)

    def test_dependencies(self):
        """
        We have a few considerations when invoking dependencies:
        - Dependency by itself (should select all output columns implicitly from the dependency without any aliasing)
        - Dependency with alias
        - Dependency with select
        - Dependency with select and alias
        - Dependency with alias and select
        This test just makes sure they all work as expected.
        """
        df = pd.DataFrame(
            {
                "fruit": ["banana", "apple"],
                "color": ["yellow", "red"],
                "size": ["medium", "medium"],
            }
        )

        @node(type="pandas")
        def t1():
            return df

        @node(type="pandas", dependencies=[t1])
        def t2(t1):
            return t1

        @node(type="pandas", dependencies=[t1.alias("nonsense")])
        def t3(nonsense):
            return nonsense

        @node(type="pandas", dependencies=[t1.select("fruit", "color")])
        def t4(t1):
            return t1

        @node(
            type="pandas", dependencies=[t1.select("fruit", "color").alias("nonsense")]
        )
        def t5(nonsense):
            return nonsense

        @node(
            type="pandas", dependencies=[t1.alias("nonsense").select("fruit", "color")]
        )
        def t6(nonsense):
            return nonsense

        assert_frame_equal(t2.run(parallel=False), df)
        assert_frame_equal(t3.run(parallel=False), df)
        assert_frame_equal(t4.run(parallel=False), df[["color", "fruit"]])
        assert_frame_equal(t5.run(parallel=False), df[["color", "fruit"]])
        assert_frame_equal(t6.run(parallel=False), df[["color", "fruit"]])

    def test_conversion_after_output_column_filter(self, spark, mocker):
        """
        a) When processing the output of a node we only select columns which are requested by child nodes.
        b) When processing a child node we convert all incoming input dataframes from parent nodes to the same type as
        the child node.

        We want to ensure that we do b) the filtering done in a) is already done. This is vital because sometimes the
        original output of a node can be far bigger than the output with filtered columns, if the order of operations
        is wrong and b) happens before a) it can be extremely inefficient.
        """

        @node(type="pandas", dependencies=[Spark("dummy_table1").select("c1")])
        def t1(dummy_table1):
            return dummy_table1

        spy = mocker.spy(DataFrameConverter, "convert")
        t1.run(spark, parallel=False)
        assert spy.call_args.args[1].columns == ["c1"]

    def test_alias_run(self):
        """
        Ensure we can set up a node dependency with an alias.
        """

        @node(type="pandas")
        def t1():
            return pd.DataFrame(
                {"fruit": ["banana", "apple"], "color": ["yellow", "red"]}
            )

        @node(type="pandas", dependencies=[t1.select("fruit").alias("my_fruits")])
        def t2(my_fruits):
            return my_fruits

        # No assertions are required, if the alias doesn't work then t2 will crash when run as the argument signature
        # won't align with what it's expecting.
        t2.run(parallel=False)

    @pytest.mark.parametrize(
        "extra_run_config,expected_df_type",
        [
            ({}, DataFrameType.PANDAS_ON_SPARK),
            ({"pandas_on_spark_use_pandas": False}, DataFrameType.PANDAS_ON_SPARK),
            ({"pandas_on_spark_use_pandas": True}, DataFrameType.PANDAS),
        ],
    )
    def test_input_dataframes_type(
        self, spark, mocker, extra_run_config, expected_df_type
    ):
        stub = mocker.stub()

        @node(type="pandas")
        def t1():
            return pd.DataFrame(
                {"fruit": ["Banana", "Apple"], "color": ["Yellow", "Red"]}
            )

        @node(type="pyspark")
        def t2():
            return spark.createDataFrame(
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

        t3.run(spark, parallel=False, **extra_run_config)
        assert dataframe_type(stub.call_args[0][0]) == expected_df_type
        assert dataframe_type(stub.call_args[0][1]) == expected_df_type

    def test_key(self):
        """
        Ensure that different nodes with the same function name have different keys
        """

        class A:
            @classmethod
            @node(type="pandas", output=Schema([Column("fruit", String(), "")]))
            def test(cls):
                return pd.DataFrame({"fruit": ["banana"]})

        class B:
            class C:
                @classmethod
                @node(
                    type="pandas",
                    dependencies=[A.test.select("fruit")],
                    output=Schema([Column("fruit", String(), "")]),
                )
                def test(cls, test):
                    return test["fruit"]

        assert (
            A.test.key
            == "flypipe_node_test_function_test_TestNode_test_key__locals__A_test"
        )
        assert (
            B.C.test.key
            == "flypipe_node_test_function_test_TestNode_test_key__locals__B_C_test"
        )

    def test_duplicated_selected(self):
        """
        Ensure throw exception if selected duplicated columns
        """

        @node(type="pandas")
        def t1():
            return pd.DataFrame(
                {"fruit": ["banana", "apple"], "color": ["yellow", "red"]}
            )

        with pytest.raises(ValueError):

            @node(
                type="pandas",
                dependencies=[t1.select("fruit", "fruit").alias("my_fruits")],
            )
            def t2(my_fruits):
                return my_fruits

    def test_alias_run_with_keys_and_alias_in_function(self):
        """
        Ensure that node graph is processed with node keys and alias is used for arguments
        """
        from flypipe.tests.transformations.group_1.t1 import (
            t1,
        )

        from flypipe.tests.transformations.group_2.t1 import (
            t1 as t1_group2,
        )

        @node(
            type="pandas",
            dependencies=[t1.select("c1"), t1_group2.select("c1").alias("t1_group2")],
            output=Schema(
                [
                    Column("c1_group1_t1", String(), "dummy"),
                    Column("c1_group2_t1", String(), "dummy"),
                ]
            ),
        )
        def t3(t1, t1_group2):
            t1["c1_group1_t1"] = t1["c1"]
            t1["c1_group2_t1"] = t1_group2["c1"]

            return t1

        df = t3.run(parallel=False)
        assert df.loc[0, "c1_group1_t1"] == "t0 group_1_t1"
        assert df.loc[0, "c1_group2_t1"] == "t0 group_2_t1"

        t1_df = pd.DataFrame(data={"c1": ["t0 group_1_t1"]})
        t1_group2_df = pd.DataFrame(data={"c1": ["t0 group_2_t1"]})

        df = t3.run(parallel=False, inputs={t1: t1_df, t1_group2: t1_group2_df})

        assert df.loc[0, "c1_group1_t1"] == "t0 group_1_t1"
        assert df.loc[0, "c1_group2_t1"] == "t0 group_2_t1"

    def test_run_dataframe_conversion(self, spark):
        """
        If a node is dependant upon a node of a different dataframe type, then we expect the output of the parent node
        to be converted when it's provided to the child node.
        """

        @node(type="pandas_on_spark", output=Schema([Column("c1", Decimal(10, 2))]))
        def t1():
            return spark.createDataFrame(
                pd.DataFrame(data={"c1": [1], "c2": [2], "c3": [3]})
            ).pandas_api()

        @node(type="pandas", dependencies=[t1.select("c1")])
        def t2(t1):
            return t1

        t1_output = t1.run(spark, parallel=False)
        t2_output = t2.run(spark, parallel=False)
        assert isinstance(t1_output, ps.frame.DataFrame)
        assert isinstance(t2_output, pd.DataFrame)

    def test_run_input_dataframes_isolation(self):
        """
        Suppose we have a node with an output x. We provide this output as input to a second node and do some tweaks to
        it. We want to ensure that the basic output is not affected by the tweaks done by the second node.
        """

        @node(
            type="pandas",
            output=Schema([Column("c1", String()), Column("c2", String())]),
        )
        def t1():
            return pd.DataFrame(data={"c1": ["1"], "c2": ["2"]})

        @node(
            type="pandas",
            dependencies=[t1.select("c1", "c2")],
            output=Schema(
                [
                    Column("c1", String(), "dummy"),
                    Column("c2", String(), "dummy"),
                    Column("c3", String(), "dummy"),
                ]
            ),
        )
        def t2(t1):
            t1["c1"] = "t2 set this value"
            t1["c3"] = t1["c1"]
            return t1

        @node(
            type="pandas",
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
            assert list(t1.columns) == ["c1", "c2"]
            assert t1.loc[0, "c1"] == "1"
            assert t1.loc[0, "c2"] == "2"
            assert list(t2.columns) == ["c1", "c2", "c3"]
            assert t2.loc[0, "c1"] == "t2 set this value"
            assert t2.loc[0, "c2"] == "2"
            assert t2.loc[0, "c3"] == "t2 set this value"
            return t2

        t3.run(parallel=False)

    def test_adhoc_call(self, spark):
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

        df = spark.createDataFrame(schema=("c1",), data=[(1,)])
        expected_df = spark.createDataFrame(schema=("c1",), data=[(2,)])

        assert_pyspark_df_equal(t2(df), expected_df)

    def test_run_skip_input(self):
        """
        If we manually provide a dataframe via the input function prior to running a node then dependent transformation
        that ordinarily generates the input dataframe should be skipped.
        """

        @node(
            type="pandas",
            output=Schema(
                [
                    Column("c1", Integer(), "dummy"),
                    Column("c2", Integer(), "dummy"),
                ]
            ),
        )
        def t1():
            raise Exception("I shouldnt be run!")

        @node(
            type="pandas",
            dependencies=[t1.select("c1")],
            output=Schema([Column("c1", Integer(), "dummy")]),
        )
        def t2(t1):
            t1["c1"] = t1["c1"] + 1
            return t1

        df = pd.DataFrame({"c1": [1]})
        expected_df = pd.DataFrame({"c1": [2]})

        assert_frame_equal(t2.run(inputs={t1: df}, parallel=False), expected_df)

    def test_run_skip_input_2(self):
        """
        When we provide a dependency input to a node, not only does that node not need to be run but we also expect any
        dependencies of the provided node not to be run.

        a - b
              \
               c
              /
            d
        When b is provided and we process c, only c and d should be run.
        """

        @node(
            type="pandas",
            output=Schema(
                [Column("c1", Integer(), "dummy"), Column("c2", Integer(), "dummy")]
            ),
        )
        def a():
            raise Exception("I shouldnt be run!")

        @node(
            type="pandas",
            dependencies=[a.select("c1")],
            output=Schema([Column("c1", Integer(), "dummy")]),
        )
        def b(a):
            a["c1"] = a["c1"] + 1
            return a

        @node(type="pandas", output=Schema([Column("c1", Integer(), "dummy")]))
        def d():
            return pd.DataFrame({"c1": [6, 7]})

        @node(
            type="pandas",
            dependencies=[b.select("c1"), d.select("c1")],
            output=Schema([Column("c1", Integer())]),
        )
        def c(b, d):
            return pd.concat([b, d], ignore_index=True)

        df = pd.DataFrame({"c1": [4, 5]})
        expected_df = pd.DataFrame({"c1": [4, 5, 6, 7]})

        assert_frame_equal(
            c.run(inputs={b: df}, parallel=False), expected_df, check_dtype=False
        )

    def test_run_missing_column(self):
        """
        If the schema requests a column which the output dataframe does not provide we expect it to error.
        """

        @node(
            type="pandas",
            output=Schema(
                [
                    Column("c1", String()),
                    Column("c2", String()),
                ]
            ),
        )
        def t1():
            return pd.DataFrame({"c1": ["Hello", "World"]})

        with pytest.raises(DataFrameMissingColumns):
            t1.run(parallel=False)

    def test_node_description_from_docstring(self):
        """
        The node description should use the docstring if not provided via a parameter.
        """

        @node(type="pandas")
        def t1():
            """
            This is a test
            """
            return

        assert t1.description == "This is a test"

    def test_node_description_from_parameter(self):
        """
        When description is provided as a parameter to the node, we expect the node description to be set accordingly,
        and for it to take precedence over the docstring description.
        """

        @node(
            type="pandas",
            description="This is another test",
        )
        def t1():
            """
            This is a test
            """
            return

        assert t1.description == "This is another test"

    def test_node_mandatory_description(self):
        with pytest.raises(ValueError) as ex, config_context(
            require_node_description=True
        ):

            @node(
                type="pandas",
            )
            def transformation():
                return

        assert str(ex.value) == (
            "Node description configured as mandatory but no description provided for node transformation"
        )

    def test_node_function(self):
        """
        Where we use a node function we expect it to be replaced with the node it returns.

        Also, the node function should function with the requested_columns parameter. If requested_columns is set to
        true then we expect the node function to receive the superset of requested columns. This is very important as
        it will allow creation of dynamic nodes that adjusts functionality based on what columns have been requested.
        """
        df = pd.DataFrame(
            {
                "fruit": ["mango", "strawberry", "banana", "pear"],
                "category": ["tropical", "temperate", "tropical", "temperate"],
                "color": ["yellow", "red", "yellow", "green"],
                "size": ["medium", "small", "medium", "medium"],
                "misc": ["bla", "bla", "bla", "bla"],
            }
        )

        @node(
            type="pandas",
        )
        def t1():
            return df

        @node_function(requested_columns=True, node_dependencies=[t1])
        def get_fruit_columns(requested_columns):
            @node(type="pandas", dependencies=[t1.select(requested_columns)])
            def t2(t1):
                return t1

            assert set(requested_columns) == {"fruit", "category", "color"}
            return t2

        @node(
            type="pandas", dependencies=[get_fruit_columns.select("fruit", "category")]
        )
        def fruit_category(get_fruit_columns):
            return get_fruit_columns

        @node(type="pandas", dependencies=[get_fruit_columns.select("fruit", "color")])
        def fruit_color(get_fruit_columns):
            return get_fruit_columns

        @node(
            type="pandas",
            dependencies=[
                fruit_category.select("fruit", "category"),
                fruit_color.select("fruit", "color"),
                t1.select("fruit", "misc"),
            ],
        )
        def fruit_details(fruit_category, fruit_color, t1):
            return fruit_category.merge(fruit_color).merge(t1)

        results = fruit_details.run(parallel=False)
        assert_frame_equal(results, df[["category", "fruit", "color", "misc"]])

    def test_node_function_series(self):
        """
        Node functions should be able to be dependent on other node functions without issue.
        """

        @node_function()
        def g1():
            @node(type="pandas")
            def t1():
                return pd.DataFrame({"c1": [1, 2, 3]})

            return t1

        @node_function(node_dependencies=[g1])
        def g2():
            @node(type="pandas", dependencies=[g1])
            def t2(g1):
                g1["c1"] *= 2
                return g1

            return t2

        @node_function(node_dependencies=[g1, g2])
        def g3():
            @node(type="pandas", dependencies=[g1, g2])
            def t3(g1, g2):
                g2["c1"] = g1["c1"] + g2["c1"]
                return g2

            return t3

        assert_frame_equal(g3.run(parallel=False), pd.DataFrame({"c1": [3, 6, 9]}))

    def test_node_function_nested(self):
        """
        Ensure we can do nested node functions- i.e node functions that return other node functions without issue.
        """
        df = pd.DataFrame({"c1": ["Bob", "Fred"], "c2": [10, 20]})

        @node(type="pandas")
        def t1():
            return df

        @node_function(node_dependencies=[t1])
        def nf1():
            @node_function(node_dependencies=[t1])
            def nf2():
                @node(type="pandas", dependencies=[t1.select("c1")])
                def t2(t1):
                    return t1

                return t2

            return nf2

        @node(type="pandas", dependencies=[nf1])
        def t3(nf1):
            return nf1

        with pytest.raises(ValueError):
            t3.run()

    def test_run_isolated_dependencies_pandas(self):
        """
        When we pass a dataframe dependency from an ancestor node to a child node the dataframe should be completely
        isolated. That is, any changes to the input dataframe should not modify the output of the parent node.
        """

        @node(type="pandas")
        def t1():
            return pd.DataFrame({"c1": [1]})

        @node(type="pandas", dependencies=[t1])
        def t2(t1):
            t1["c1"] += 1
            return t1

        @node(type="pandas", dependencies=[t1, t2])
        def t3(t1, t2):
            # t1 returns 1, t2 adds 1 to t1 (returning 2), t3 adds t1 and t2. t2 adding 1 to t1 should not modify the
            # output of t1
            assert t1.loc[0, "c1"] == 1
            assert t2.loc[0, "c1"] == 2
            return t1

        t3.run(parallel=False)

    def test_run_isolated_dependencies_pandas_on_spark(self, spark):
        """
        When we pass a dataframe dependency from an ancestor node to a child node the dataframe should be completely
        isolated. That is, any changes to the input dataframe should not modify the output of the parent node.
        """

        @node(type="pandas_on_spark")
        def t1():
            return spark.createDataFrame(data=[{"c1": 1}]).pandas_api()

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

        t3.run(parallel=False)

    def test_run_isolated_dependencies_spark(self, spark):
        """
        When we pass a dataframe dependency from an ancestor node to a child node the dataframe should be completely
        isolated. That is, any changes to the input dataframe should not modify the output of the parent node.
        """

        @node(
            type="pyspark",
        )
        def t1():
            return spark.createDataFrame(data=[{"c1": 1}])

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

        t3.run(parallel=False)

    def test_pandas_on_spark_use_pandas(self, spark):
        """
        When running a graph with pandas_on_spark_use_pandas=True, all pandas_on_spark nodes types should be of type
        pandas. If running straight after, but with pandas_on_spark_use_pandas=False, all pandas_on_spark nodes types
        should be of type pandas_on_spark.
        """

        @node(type="pandas_on_spark", dependencies=[Spark("dummy_table1").select("c1")])
        def t1(dummy_table1):
            return dummy_table1

        t1.run(spark, pandas_on_spark_use_pandas=True)
        assert t1.dataframe_type == DataFrameType.PANDAS_ON_SPARK

        t1.run(spark, pandas_on_spark_use_pandas=False)
        assert t1.dataframe_type == DataFrameType.PANDAS_ON_SPARK

        # TODO- we should refactor to avoid having to call this private method to build a graph
        t1.create_graph(RunContext(pandas_on_spark_use_pandas=True))
        for n in t1.node_graph.graph.nodes:
            if t1.node_graph.graph.nodes[n]["transformation"].__name__ == "t1":
                assert (
                    t1.node_graph.graph.nodes[n]["transformation"].dataframe_type
                    == DataFrameType.PANDAS
                )

    def test_function_argument_signature(self, spark):
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
            return spark.createDataFrame(data=[{"c1": 1, "c2": 2}])

        @node(
            type="pyspark",
            dependencies=[t1.select("c1")],
            requested_columns=True,
            spark_context=True,
        )
        def t2(t1, requested_columns, spark):
            assert requested_columns == ["c1"]
            assert dataframe_type(t1) == DataFrameType.PYSPARK
            assert t1.columns == ["c1"]
            assert isinstance(spark, pyspark.sql.session.SparkSession)
            return t1

        @node(
            type="pyspark",
            dependencies=[
                t1.select("c1", "c2"),
                t2.select("c1"),
            ],
            spark_context=True,
        )
        def t3(t2, t1, spark):
            assert dataframe_type(t1) == DataFrameType.PYSPARK
            assert t1.columns == ["c1", "c2"]

            assert dataframe_type(t2) == DataFrameType.PYSPARK
            assert t2.columns == ["c1"]

            assert isinstance(spark, pyspark.sql.session.SparkSession)

            return t1

        t3.run(spark, parallel=False)

    def test_run_one_dependency_multiple_instances(self):
        """
        If our graph has multiple instances of the same dependency then they should be treated as seperate copies.
            b
          /   \
        a - - - c
        """

        @node(type="pandas")
        def a():
            return pd.DataFrame({"c1": [1], "c2": [1]})

        @node(type="pandas", dependencies=[a.select("c1")])
        def b(a):
            return a

        @node(type="pandas", dependencies=[a.select("c2"), b])
        def c(a, b):
            return a

        c.run(parallel=False)

    def test_node_parameters(self):
        @node(type="pandas")
        def t1(param1=1, param2=2):
            assert param1 == 10 and param2 == 20
            return pd.DataFrame()

        t1.run(parameters={t1: {"param1": 10, "param2": 20}})

    def test_node_parameters_missing(self):
        @node(type="pandas")
        def t1(param1):
            return pd.DataFrame()

        with pytest.raises(TypeError):
            t1.run()

    def test_spark_sql_conversion(self, spark):
        """
        If we use a spark sql code then:
        - any dependencies should be saved into a unique table.
        - the dataframes passed into the spark sql function should be replaced with the names of the tables they were
        saved into.
        """

        @node(type="pandas")
        def t1():
            return pd.DataFrame({"number": [1]})

        @node(type="spark_sql", dependencies=[t1])
        def t2(t1):
            return f"select number+1 from {t1}"

        with mock.patch.object(
            DataFrame, "createOrReplaceTempView"
        ) as createOrReplaceTempView_mock, mock.patch.object(
            spark, "sql", return_value=spark.createDataFrame(data=[{"number": 2}])
        ) as sql_mock:
            t2.run(spark=spark)
        assert sql_mock.call_args[0][0] == "select number+1 from t2__t1"
        assert createOrReplaceTempView_mock.call_args[0][0] == "t2__t1"

    def test_spark_sql_select_column(self, spark):
        """
        Basic test for spark sql where the dependency has a select column in it.
        There was a bug (DATA-3936) where this use case stacktraces.
        """

        @node(type="pandas")
        def t1():
            return pd.DataFrame({"c1": ["chris"]})

        @node(type="spark_sql", dependencies=[t1.select("c1")])
        def t2(t1):
            return f"select * from {t1}"

        with mock.patch.object(
            DataFrame, "createOrReplaceTempView"
        ) as createOrReplaceTempView_mock, mock.patch.object(
            spark, "sql", return_value=spark.createDataFrame(data=[{"number": 2}])
        ) as sql_mock:
            t2.run(spark=spark)
        assert sql_mock.call_args[0][0] == "select * from t2__t1"
        assert createOrReplaceTempView_mock.call_args[0][0] == "t2__t1"

    def test_node_dependency_used_multiple_times(self):
        """
        We should throw an exception if the user attempts to use the same node as a dependency multiple times on a
        node.
        """

        @node(
            type="pandas",
        )
        def t1():
            return pd.DataFrame({"c1": [1, 2, 3], "c2": ["apple", "orange", "plum"]})

        with pytest.raises(ValueError):

            @node(
                type="pandas",
                dependencies=[
                    t1.select("c1").alias("a"),
                    t1.select("c2").alias("b"),
                ],
            )
            def t2(a, b):
                return pd.concat([a, b], axis=1)

    def test_node_duplicate_name(self):
        """
        Having multiple dependencies with the same name/alias should fail.
        """

        @node(type="pandas")
        def t1():
            return pd.DataFrame({"c1": [1, 2, 3]})

        @node(type="pandas")
        def t2():
            return pd.DataFrame({"c2": [1, 2, 3]})

        with pytest.raises(ValueError):

            @node(type="pandas", dependencies=[t1, t2.alias("t1")])
            def t3(t1):
                return t1

        with pytest.raises(ValueError):

            @node(type="pandas", dependencies=[t1.alias("test"), t2.alias("test")])
            def t4(test):
                return test

    def test_run_parallel(self):
        @node(type="pandas")
        def t1():
            return pd.DataFrame({"c1": [3]})

        @node(type="pandas", dependencies=[t1])
        def t2(t1):
            return t1 + 1

        @node(type="pandas", dependencies=[t1])
        def t3(t1):
            return t1 * 10

        @node(type="pandas", dependencies=[t1])
        def t4(t1):
            return t1**2

        @node(type="pandas", dependencies=[t2, t3, t4])
        def t5(t2, t3, t4):
            return pd.concat([t2, t3, t4]).reset_index(drop=True)

        result = t5.run(parallel=True)
        assert_frame_equal(result, pd.DataFrame({"c1": [4, 30, 9]}))

    def test_select(self):
        """
        Assert only selected columns (ordered) are being selected
        """

        @node(type="pandas")
        def t0():
            return pd.DataFrame(data={"col1": [1], "col2": [2], "col3": [3]})

        input_node = t0.select("col3", "col2")

        assert input_node._selected_columns == [
            "col2",
            "col3",
        ]

    def test_output(self):
        @node(
            type="pandas", output=Schema([Column("col1", String(), "description col1")])
        )
        def t0():
            return pd.DataFrame(data={"col1": [1]})

        assert isinstance(t0.output_schema, Schema)

        columns_t0 = t0.output_schema.columns[0]

        assert columns_t0.name == "col1"
        assert isinstance(columns_t0.type, String)
        assert columns_t0.description == "description col1"

    def test_alias(self):
        @node(type="pandas")
        def t0():
            return pd.DataFrame(data={"col1": [1]})

        input_node = t0.alias("my_alias")
        assert input_node.key == t0.key
        assert input_node.get_alias() == "my_alias"

    def test_cache_instance(self):
        class MyCacheNoSuperclass:
            pass

        with pytest.raises(TypeError):

            @node(type="pandas", cache=MyCacheNoSuperclass())
            def t0():
                return pd.DataFrame(data={"col1": [1]})

        class MyCache(Cache):
            def read(self):
                pass

            def write(self):
                pass

            def exists(self):
                pass

        @node(type="pandas", cache=MyCache())
        def t1():
            return pd.DataFrame(data={"col1": [1]})
