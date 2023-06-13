import os

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from flypipe import node_function
from flypipe.cache import CacheMode
from flypipe.cache.cache import Cache
from flypipe.node import node
from flypipe.node_function import NodeFunction
from flypipe.node_graph import RunStatus
from flypipe.schema import Schema, Column
from flypipe.schema.types import Integer


@pytest.fixture(scope="function")
def spark():
    from flypipe.tests.spark import spark  # pylint: disable=import-outside-toplevel

    return spark


@pytest.fixture(autouse=True)
def clean_up():
    if os.path.exists("test.csv"):
        os.remove("test.csv")

    yield

    if os.path.exists("test.csv"):
        os.remove("test.csv")


class GenericCache(Cache):
    def read(self):
        return pd.read_csv("test.csv")

    def write(self, df):
        df.to_csv("test.csv", index=False)

    def exists(self):
        return os.path.exists("test.csv")


@pytest.fixture(scope="function")
def cache():  # pylint: disable=duplicate-code
    return GenericCache()


class TestCache:
    """Unit tests on the Cache class"""

    def test_cache(self):
        class MyCache(Cache):
            def read(self):
                pass

            def write(self):
                pass

            def exists(self):
                pass

        MyCache()

    def test_cache_inheritance(self):
        class MyCache(Cache):
            pass

        with pytest.raises(TypeError):
            MyCache()

    def test_cache_non_spark_trivial(self):

        class GenericCache2(GenericCache):
            def write(self, df):
                df = pd.DataFrame(data={"col1": [1]})
                df.to_csv("test.csv", index=False)

        cache = GenericCache2()

        @node(
            type="pandas",
            cache=cache,
        )
        def t1():
            return pd.DataFrame(data={"col1": [1], "col2": [2]})

        df = t1.run()
        assert sorted(list(df.columns)) == ["col1", "col2"]
        df = t1.run()
        assert sorted(list(df.columns)) == ["col1"]

    def test_cache_non_spark(self, cache, mocker):
        @node(
            type="pandas",
            cache=cache,
        )
        def t1():
            return pd.DataFrame(data={"col1": [1], "col2": [2]})

        spy_writter = mocker.spy(t1.cache, "write")
        spy_reader = mocker.spy(t1.cache, "read")
        spy_exists = mocker.spy(t1.cache, "exists")
        t1.run()
        assert spy_writter.call_count == 1
        assert spy_reader.call_count == 0
        assert spy_exists.call_count == 1

        spy_writter.reset_mock()
        spy_reader.reset_mock()
        spy_exists.reset_mock()
        t1.run()
        assert spy_writter.call_count == 0
        assert spy_reader.call_count == 1
        assert spy_exists.call_count == 1

    def test_cache_spark_provided(self, spark, mocker):

        class GenericCache2(GenericCache):
            def read(self, spark):
                return (
                    spark.read.option("inferSchema", True)
                    .option("header", True)
                    .csv("test.csv")
                )

            def write(self, spark, df):
                df.toPandas().to_csv("test.csv", index=False)

            def exists(self, spark):
                if os.path.exists("test.csv"):
                    return spark.read.option("header", True).csv("test.csv").count() > 0

                return False

        cache = GenericCache2()

        @node(type="pyspark", cache=cache, spark_context=True)
        def t1(spark):
            return spark.createDataFrame(
                schema=("c0", "c1"),
                data=[
                    (
                        0,
                        1,
                    )
                ],
            )

        spy_writter = mocker.spy(cache, "write")
        spy_reader = mocker.spy(cache, "read")
        spy_exists = mocker.spy(cache, "exists")
        t1.run(spark)
        assert spy_writter.call_count == 1
        assert spy_reader.call_count == 0
        assert spy_exists.call_count == 1

        spy_writter.reset_mock()
        spy_reader.reset_mock()
        spy_exists.reset_mock()
        t1.run(spark)
        assert spy_writter.call_count == 0
        assert spy_reader.call_count == 1
        assert spy_exists.call_count == 1

    def test_query_cache(self, cache):
        """

        t0 (skipped)
           \
           t2 (cached) -- t3
          /
        t1 (skipped)


        """

        @node(type="pandas")
        def t0():
            return pd.DataFrame(data={"t0": [1]})

        @node(type="pandas")
        def t1():
            return pd.DataFrame(data={"t1": [1]})

        @node(type="pandas", cache=cache, dependencies=[t0, t1])
        def t2(t0, t1):  # pylint: disable=unused-argument
            return t0

        @node(
            type="pandas",
            dependencies=[t2],
        )
        def t3(t2):
            return t2

        # to write cache
        t3.run()

        # to read cache
        t3.run()
        for node_name in t3.node_graph.graph.nodes:
            node_graph = t3.node_graph.get_node(node_name)
            if node_graph["transformation"].function.__name__ == "t3":
                assert node_graph["status"] == RunStatus.ACTIVE
            else:
                assert node_graph["status"] == RunStatus.SKIP

    def test_query_cache1(self, cache):
        """

        t0 (skipped)
           \
           t2 (cached) ---  t3 (active)
          /                /
        t1 (active) -- t4 (active)


        """

        @node(type="pandas")
        def t0():
            return pd.DataFrame(data={"t0": [1]})

        @node(type="pandas")
        def t1():
            return pd.DataFrame(data={"t1": [1]})

        @node(type="pandas", dependencies=[t1])
        def t4(t1):
            return t1

        @node(type="pandas", cache=cache, dependencies=[t0, t1])
        def t2(t0, t1):  # pylint: disable=unused-argument
            return t0

        @node(
            type="pandas",
            dependencies=[t2, t4],
        )
        def t3(t2, t4):  # pylint: disable=unused-argument
            return t2

        # to write cache
        t3.run()

        # to read cache
        t3.run()

        for node_name in t3.node_graph.graph.nodes:
            node_graph = t3.node_graph.get_node(node_name)
            if node_graph["transformation"].function.__name__ in ["t3", "t4", "t1"]:
                assert node_graph["status"] == RunStatus.ACTIVE
            else:
                assert node_graph["status"] == RunStatus.SKIP

    def test_cache_non_spark_provided_input(self, cache, mocker):
        """
        If input is provided, it does not uses caches at all.
        """

        @node(
            type="pandas",
            cache=cache,
        )
        def t1():
            return pd.DataFrame(data={"col1": [1], "col2": [2]})

        spy_writter = mocker.spy(cache, "write")
        spy_reader = mocker.spy(cache, "read")
        spy_exists = mocker.spy(cache, "exists")
        t1.run(inputs={t1: pd.DataFrame(data={"col1": [1]})})
        assert spy_writter.call_count == 0
        assert spy_reader.call_count == 0
        assert spy_exists.call_count == 0

    def test_cache_non_spark_provided_input_node_function(self, cache, mocker):
        """
        If input is provided for a node function, it does not uses caches at all.
        """

        @node_function()
        def t1():
            @node(
                type="pandas",
                cache=cache,
            )
            def t1():
                return pd.DataFrame(data={"col1": [1], "col2": [2]})

            return t1

        spy_writter = mocker.spy(cache, "write")
        spy_reader = mocker.spy(cache, "read")
        spy_exists = mocker.spy(cache, "exists")
        t1.run(inputs={t1: pd.DataFrame(data={"col1": [1]})})
        assert spy_writter.call_count == 0
        assert spy_reader.call_count == 0
        assert spy_exists.call_count == 0

    def test_cache_node_function(self, cache, mocker):
        @node_function()
        def t1f():
            @node(
                type="pandas",
                cache=cache,
            )
            def t1():
                return pd.DataFrame(data={"col1": [1], "col2": [2]})

            return t1

        spy_writter = mocker.spy(cache, "write")
        spy_reader = mocker.spy(cache, "read")
        spy_exists = mocker.spy(cache, "exists")
        t1f.run()
        assert spy_writter.call_count == 1
        assert spy_reader.call_count == 0
        assert spy_exists.call_count == 1

        spy_writter.reset_mock()
        spy_reader.reset_mock()
        spy_exists.reset_mock()
        df = t1f.run()
        assert isinstance(df, pd.DataFrame)
        assert spy_writter.call_count == 0
        assert spy_reader.call_count == 1
        assert spy_exists.call_count == 1

    # pylint: disable=too-many-statements
    def test_cases(self, cache, mocker):
        """

        | Case | Input Provided | Cache Disabled? | Cache has been written? | Action |
        | --- | --- | --- | --- | --- |
        | 1 | Yes | Any | Any | return provided input (do not run transformation, neither query/write cache) |
        | 2 | No | Yes | No | Run transformation (do not save cache) |
        | 3 | No | No | No | Run transformation and save cache |
        | 4 | No | No | Yes | query existing cache (do not run transformation) |
        | 5 | No | Yes | Yes | Run transformation (do not query cache or save cache) |



        """

        t_df = pd.DataFrame(data={"col1": [1]})

        @node(type="pandas", cache=cache)
        def t():
            return t_df

        spy_writter = mocker.spy(cache, "write")
        spy_reader = mocker.spy(cache, "read")
        spy_exists = mocker.spy(cache, "exists")

        # Case 1
        spy_writter.reset_mock()
        spy_reader.reset_mock()
        spy_exists.reset_mock()

        input_df = pd.DataFrame(data={"col1": [2]})
        output_df = t.run(inputs={t: input_df})
        assert_frame_equal(input_df, output_df)

        assert spy_writter.call_count == 0
        assert spy_reader.call_count == 0
        assert spy_exists.call_count == 0

        # Case 2
        spy_writter.reset_mock()
        spy_reader.reset_mock()
        spy_exists.reset_mock()

        assert not os.path.exists("test.csv")
        output_df = t.run(cache={t: CacheMode.DISABLE})
        assert_frame_equal(t_df, output_df)
        assert not os.path.exists("test.csv")
        assert spy_writter.call_count == 0
        assert spy_reader.call_count == 0
        assert spy_exists.call_count == 0

        # Case 3
        spy_writter.reset_mock()
        spy_reader.reset_mock()
        spy_exists.reset_mock()

        assert not os.path.exists("test.csv")
        output_df = t.run()
        assert_frame_equal(t_df, output_df)
        assert os.path.exists("test.csv")

        assert spy_writter.call_count == 1
        assert spy_reader.call_count == 0
        assert spy_exists.call_count == 1

        # Case 4
        spy_writter.reset_mock()
        spy_reader.reset_mock()
        spy_exists.reset_mock()

        assert os.path.exists("test.csv")
        output_df = t.run()
        assert_frame_equal(t_df, output_df)
        assert os.path.exists("test.csv")

        assert spy_writter.call_count == 0
        assert spy_reader.call_count == 1
        assert spy_exists.call_count == 1

        # Case 5
        spy_writter.reset_mock()
        spy_reader.reset_mock()
        spy_exists.reset_mock()

        assert os.path.exists("test.csv")
        output_df = t.run(cache={t: CacheMode.DISABLE})
        assert_frame_equal(t_df, output_df)
        assert os.path.exists("test.csv")

        assert spy_writter.call_count == 0
        assert spy_reader.call_count == 0
        assert spy_exists.call_count == 0

    def test_cache_merge(self, mocker):

        class MyCache(Cache):
            def read(self):
                return pd.read_csv("test.csv")

            def write(self, df):
                if self.exists():
                    df = pd.DataFrame(data={"col1": [1, 2], "col2": [2, 3]})
                    df.to_csv("test.csv", index=False)
                else:
                    df.to_csv("test.csv", index=False)

            def exists(self):
                return os.path.exists("test.csv")

        cache = MyCache()

        @node(
            type="pandas",
            cache=cache,
        )
        def t1():
            return pd.DataFrame(data={"col1": [1], "col2": [2]})

        spy_writter = mocker.spy(t1.cache, "write")
        spy_reader = mocker.spy(t1.cache, "read")
        spy_exists = mocker.spy(t1.cache, "exists")
        t1.run()

        assert spy_writter.call_count == 1
        assert spy_reader.call_count == 0
        assert spy_exists.call_count == 2

        spy_writter.reset_mock()
        spy_reader.reset_mock()
        spy_exists.reset_mock()
        t1.run(cache={t1: CacheMode.MERGE})

        assert spy_writter.call_count == 1
        assert spy_reader.call_count == 0
        assert spy_exists.call_count == 1

    def test_cache_requested_columns(self, cache, mocker):
        @node(
            type="pandas",
            cache=cache,
            output=Schema(
                Column("col1", Integer()),
                Column("col2", Integer()),
            )
        )
        def t1():
            return pd.DataFrame(data={"col1": [1], "col2": [2], "col3": [3]})

        @node(
            type="pandas",
            dependencies=[t1.select("col1")],
        )
        def t2(t1):
            return t1

        assert not os.path.exists("test.csv")
        spy_writter = mocker.spy(t1.cache, "write")
        spy_reader = mocker.spy(t1.cache, "read")
        spy_exists = mocker.spy(t1.cache, "exists")
        t1.run()
        assert spy_writter.call_count == 1
        assert spy_reader.call_count == 0
        assert spy_exists.call_count == 1

        assert os.path.exists("test.csv")
        df = pd.read_csv("test.csv")
        assert sorted(list(df.columns)) == ["col1", "col2"]

    def test_node_function_cache(self, cache):
        """
        t0 Function (t0 node) --> t1 (node) --> t2 Function (t2)
        Assert input node t0 of t1 is still a NodeFunction after running t2
        """

        from flypipe import node, node_function

        @node_function()
        def t0():
            @node(
                type="pandas",
                cache=cache
            )
            def t0():
                return pd.DataFrame(data={"c1": [1]})

            return t0

        @node(
            type="pandas",
            dependencies=[t0.alias("df")],
        )
        def t1(df):
            return df

        @node_function(
            node_dependencies=[t1]
        )
        def t2():
            @node(
                type="pandas",
                dependencies=[t1.alias("df")],
            )
            def t2(df):
                return df

            return t2

        t2.run()
        assert isinstance(t1.input_nodes[0].node, NodeFunction)
        t2.run()

    def test_cache_read_node_dependencies(self, cache, mocker):
        """

        t1 (cached) -> t2 (cached)

        running t2, we expect t1 not to read cache, only t2 will read

        """
        @node(
            type="pandas",
            cache=cache,
        )
        def t1():
            return pd.DataFrame(data={"col1": [1], "col2": [2]})

        @node(
            type="pandas",
            cache=cache,
            dependencies=[t1]
        )
        def t2(t1):
            return t1

        spy_writter = mocker.spy(cache, "write")
        spy_reader = mocker.spy(cache, "read")
        spy_exists = mocker.spy(cache, "exists")
        t1.run()
        assert spy_writter.call_count == 1
        assert spy_reader.call_count == 0
        assert spy_exists.call_count == 1

        spy_writter.reset_mock()
        spy_reader.reset_mock()
        spy_exists.reset_mock()
        t2.run()
        assert spy_writter.call_count == 0
        assert spy_reader.call_count == 1
        assert spy_exists.call_count == 1