import os
import pandas as pd

from flypipe import node
from flypipe.datasource.spark import Spark

from datetime import datetime

import pytest

from flypipe.schema import Schema, Column
from flypipe.schema.types import DateTime, Integer


@node(
    type="pandas",
)
def transformation():
    return pd.DataFrame({"c1": [1, 2], "c2": ["a", "b"], "c3": [True, False]})


def get_df():
    data = {
        "col1": [1, 2, 3],
        "datetime_created": [
            datetime(2025, 1, 1),
            datetime(2025, 1, 2),
            datetime(2025, 1, 3),
        ],
    }

    return pd.DataFrame(data).copy()


@node(
    type="pandas",
    output=Schema(
        Column("col1", Integer()),
        Column("datetime_created", DateTime()),
    ),
)
def t1():
    return get_df()


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") not in ["SPARK", "SPARK_CONNECT"],
    reason="PySpark tests require RUN_MODE=SPARK or SPARK_CONNECT",
)
class TestInputNodePySpark:
    """Tests for InputNode - PySpark"""

    def test_alias_unmodified_spark_datasource(self):
        node_input = Spark("schema_a.table_b").select("c1", "c2")
        assert node_input.get_alias() == "schema_a_table_b"

    def test_preprocess_no_argument_fails(self):
        with pytest.raises(ValueError):

            @node(type="pyspark", dependencies=[t1.preprocess()])
            def n(df):
                return df

    def test_preprocess_non_callable_argument_fails(self):

        with pytest.raises(ValueError):

            @node(type="pyspark", dependencies=[t1.preprocess(1)])
            def n(df):
                return df

    def test_preprocess_non_callable_in_list_argument_fails(self):

        with pytest.raises(ValueError):

            @node(type="pyspark", dependencies=[t1.preprocess(lambda df: df, 1)])
            def n(df):
                return df

