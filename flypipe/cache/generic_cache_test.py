import os

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from flypipe import node_function
from flypipe.cache import CacheOperation
from flypipe.cache.generic_cache import GenericCache
from flypipe.node import node
from flypipe.node_graph import RunStatus


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


def read():
    return pd.read_csv("test.csv")


def write(df):
    df.to_csv("test.csv", index=False)


def exists():
    return os.path.exists("test.csv")


@pytest.fixture(scope="function")
def cache(): # pylint: disable=duplicate-code
    return GenericCache(read=read, write=write, exists=exists)


class TestGenericCache:
    """Unit tests on the Node class"""

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
        def read(spark):
            return (
                spark.read.option("inferSchema", True)
                .option("header", True)
                .csv("test.csv")
            )

        # pylint: disable=unused-argument
        def write(spark, df):
            df.toPandas().to_csv("test.csv", index=False)

        def exists(spark):
            if os.path.exists("test.csv"):
                return spark.read.option("header", True).csv("test.csv").count() > 0

            return False

        cache = GenericCache(read=read, write=write, exists=exists, spark_context=True)

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
        def t2(t0, t1): # pylint: disable=unused-argument
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
        def t2(t0, t1): # pylint: disable=unused-argument
            return t0

        @node(
            type="pandas",
            dependencies=[t2, t4],
        )
        def t3(t2, t4): # pylint: disable=unused-argument
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

        assert not exists()
        output_df = t.run(cache={t: CacheOperation.DISABLE})
        assert_frame_equal(t_df, output_df)
        assert not exists()
        assert spy_writter.call_count == 0
        assert spy_reader.call_count == 0
        assert spy_exists.call_count == 0

        # Case 3
        spy_writter.reset_mock()
        spy_reader.reset_mock()
        spy_exists.reset_mock()

        assert not exists()
        output_df = t.run()
        assert_frame_equal(t_df, output_df)
        assert exists()

        assert spy_writter.call_count == 1
        assert spy_reader.call_count == 0
        assert spy_exists.call_count == 1

        # Case 4
        spy_writter.reset_mock()
        spy_reader.reset_mock()
        spy_exists.reset_mock()

        assert exists()
        output_df = t.run()
        assert_frame_equal(t_df, output_df)
        assert exists()

        assert spy_writter.call_count == 0
        assert spy_reader.call_count == 1
        assert spy_exists.call_count == 1

        # Case 5
        spy_writter.reset_mock()
        spy_reader.reset_mock()
        spy_exists.reset_mock()

        assert exists()
        output_df = t.run(cache={t: CacheOperation.DISABLE})
        assert_frame_equal(t_df, output_df)
        assert exists()

        assert spy_writter.call_count == 0
        assert spy_reader.call_count == 0
        assert spy_exists.call_count == 0
