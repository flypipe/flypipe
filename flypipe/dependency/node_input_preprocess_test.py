from argparse import ArgumentError, ArgumentTypeError
from datetime import datetime

import pandas as pd
import pytest
from pandas._testing import assert_frame_equal

from flypipe import node
from flypipe.cache import Cache
from flypipe.mode import PreProcessMode
from flypipe.schema import Schema, Column
from flypipe.schema.types import DateTime, Integer


class Cacher(Cache):
    def read(self):
        data = {
            "col1": [1, 2, 3],
            "datetime_created": [
                datetime(2025, 1, 1),
                datetime(2025, 1, 2),
                datetime(2025, 1, 3),
            ],
        }

        return pd.DataFrame(data).copy()

    def write(self, df):
        pass

    def exists(self):
        return True


@node(
    type="pandas",
    cache=Cacher(),
    output=Schema(
        Column("col1", Integer()),
        Column("datetime_created", DateTime()),
    ),
)
def t1(df):
    return df


def preprocess_remove_1st_day(df):
    return df[df["datetime_created"] != datetime(2025, 1, 1)]


def preprocess_remove_2nd_day(df):
    return df[df["datetime_created"] != datetime(2025, 1, 2)]


class TestNodeInputPreProcess:
    def test_preprocess_no_argument_fails(self):
        with pytest.raises(ArgumentError):

            @node(type="pyspark", dependencies=[t1.preprocess()])
            def n(df):
                return df

    def test_preprocess_non_callable_argument_fails(self):

        with pytest.raises(ArgumentTypeError):

            @node(type="pyspark", dependencies=[t1.preprocess(1)])
            def n(df):
                return df

    def test_preprocess_non_callable_in_list_argument_fails(self):

        with pytest.raises(ArgumentTypeError):

            @node(type="pyspark", dependencies=[t1.preprocess(lambda df: df, 1)])
            def n(df):
                return df

    @pytest.mark.parametrize(
        "functions,expected",
        [
            (
                preprocess_remove_1st_day,
                {
                    "col1": [2, 3],
                    "datetime_created": [datetime(2025, 1, 2), datetime(2025, 1, 3)],
                },
            ),
            (
                [preprocess_remove_1st_day],
                {
                    "col1": [2, 3],
                    "datetime_created": [datetime(2025, 1, 2), datetime(2025, 1, 3)],
                },
            ),
            (
                [preprocess_remove_1st_day, preprocess_remove_2nd_day],
                {"col1": [3], "datetime_created": [datetime(2025, 1, 3)]},
            ),
            (
                (preprocess_remove_1st_day, preprocess_remove_2nd_day),
                {"col1": [3], "datetime_created": [datetime(2025, 1, 3)]},
            ),
            (
                [preprocess_remove_2nd_day, preprocess_remove_1st_day],
                {"col1": [3], "datetime_created": [datetime(2025, 1, 3)]},
            ),
        ],
    )
    def test_preprocess_works_with_list_function(self, functions, expected):

        if not isinstance(functions, tuple):
            input = t1.preprocess(functions).alias("df")
        else:
            input = t1.preprocess(*functions).alias("df")

        @node(type="pandas", dependencies=[input])
        def n(df):
            return df

        df = n.run()
        assert_frame_equal(df.reset_index(drop=True), pd.DataFrame(expected))

    def test_preprocess_order(self):
        def order_1(df):
            df["col2"] = [1, 2, 3]
            return df

        def order_2(df):
            return df[df["col2"] == 2]

        @node(type="pandas", dependencies=[t1.preprocess(order_1, order_2).alias("df")])
        def n(df):
            return df

        df = n.run()

        expected = {
            "col1": [2],
            "datetime_created": [datetime(2025, 1, 2)],
            "col2": [2],
        }
        assert_frame_equal(df.reset_index(drop=True), pd.DataFrame(expected))

        @node(type="pandas", dependencies=[t1.preprocess(order_2, order_1).alias("df")])
        def n(df):
            return df

        with pytest.raises(KeyError):
            n.run()

    def test_preprocess_mode_disable_do_not_run(self):
        def preprocess(df):
            return df[df["datetime_created"] == datetime(2025, 1, 2)]

        @node(type="pandas", dependencies=[t1.preprocess(preprocess).alias("df")])
        def n(df):
            return df

        df = n.run(preprocess=PreProcessMode.DISABLE)
        assert_frame_equal(df, Cacher().read())

        df = n.run(preprocess={n: {t1: PreProcessMode.DISABLE}})
        assert_frame_equal(df, Cacher().read())

    def test_config_preprocess(self, monkeypatch):

        monkeypatch.setenv(
            "FLYPIPE_DEFAULT_DEPENDENCIES_PREPROCESS_MODULE",
            "flypipe.dependency.node_input_preprocess_func_test",
        )
        monkeypatch.setenv(
            "FLYPIPE_DEFAULT_DEPENDENCIES_PREPROCESS_FUNCTION", "preprocess_config"
        )

        @node(type="pandas", dependencies=[t1.alias("df")])
        def n(df):
            return df

        df = n.run()
        expected = {"col1": [2], "datetime_created": [datetime(2025, 1, 2)]}
        assert_frame_equal(df.reset_index(drop=True), pd.DataFrame(expected))

    def test_config_preprocess_with_mode_disable(self, monkeypatch):

        monkeypatch.setenv(
            "FLYPIPE_DEFAULT_DEPENDENCIES_PREPROCESS_MODULE",
            "flypipe.dependency.node_input_preprocess_func_test",
        )
        monkeypatch.setenv(
            "FLYPIPE_DEFAULT_DEPENDENCIES_PREPROCESS_FUNCTION", "preprocess_config"
        )

        @node(type="pandas", dependencies=[t1.alias("df")])
        def n(df):
            return df

        df = n.run(preprocess=PreProcessMode.DISABLE)
        assert_frame_equal(df, Cacher().read())

    def test_preprocess_upstream(self):
        def preprocess_f(df):
            return df[df["datetime_created"] == datetime(2025, 1, 2)]

        @node(type="pandas", dependencies=[t1.preprocess(preprocess_f).alias("df")])
        def n1(df):
            return df

        @node(type="pandas", dependencies=[n1.alias("df")])
        def n2(df):
            return df

        df = n2.run()
        assert_frame_equal(df, preprocess_f(Cacher().read()))

        df = n2.run(preprocess=PreProcessMode.DISABLE)
        assert_frame_equal(df, Cacher().read())

        df = n2.run(preprocess={n1: {t1: PreProcessMode.DISABLE}})
        assert_frame_equal(df, Cacher().read())

    def test_input_node_disabled_preprocess_do_not_process(self, monkeypatch):

        monkeypatch.setenv(
            "FLYPIPE_DEFAULT_DEPENDENCIES_PREPROCESS_MODULE",
            "flypipe.dependency.node_input_preprocess_func_test",
        )
        monkeypatch.setenv(
            "FLYPIPE_DEFAULT_DEPENDENCIES_PREPROCESS_FUNCTION", "preprocess_config"
        )

        @node(
            type="pandas",
            dependencies=[t1.preprocess(PreProcessMode.DISABLE).alias("df")],
        )
        def n(df):
            return df

        df = n.run()
        assert_frame_equal(df, Cacher().read())

    def test_preprocess_mode_disable_priorities_order(self, monkeypatch):
        monkeypatch.setenv(
            "FLYPIPE_DEFAULT_DEPENDENCIES_PREPROCESS_MODULE",
            "flypipe.dependency.node_input_preprocess_func_test",
        )
        monkeypatch.setenv(
            "FLYPIPE_DEFAULT_DEPENDENCIES_PREPROCESS_FUNCTION", "preprocess_config"
        )

