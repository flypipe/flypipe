import os
from uuid import uuid4

import pandas as pd
import pytest

from flypipe.cache.cache import Cache
from flypipe.node import node


class GenericCache(Cache):
    def __init__(self):
        self.cache_name = str(uuid4()).replace("-", "_")
        self.cache_csv = f"{self.cache_name}.csv"

    def read(self, from_node=None, to_node=None, is_static=False):
        return pd.read_csv(self.cache_csv)

    def write(
        self,
        *args,
        df,
        upstream_nodes=None,
        to_node=None,
        datetime_started_transformation=None,
        **kwargs,
    ):
        df.to_csv(self.cache_csv, index=False)

    def exists(self):
        return os.path.exists(self.cache_csv)


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") not in ["SPARK", "SPARK_CONNECT"],
    reason="PySpark tests require RUN_MODE=SPARK or RUN_MODE=SPARK_CONNECT",
)
class TestCachePySpark:
    """Unit tests on the Cache class - PySpark functionality"""

    def test_cache_with_session(self, spark, mocker):
        class GenericCache2(GenericCache):
            def read(self, session, from_node=None, to_node=None, is_static=False):
                return session.read.table(self.cache_name)

            def write(
                self,
                session,
                *args,
                df,
                upstream_nodes=None,
                to_node=None,
                datetime_started_transformation=None,
                **kwargs,
            ):
                df.createOrReplaceTempView(self.cache_name)

            def exists(self, session):
                try:
                    session.read.table(self.cache_name)
                    return session.table(self.cache_name).count() > 0
                except Exception:
                    return False

        cache = GenericCache2()

        @node(type="pyspark", cache=cache, session_context=True)
        def t1(session):
            return session.createDataFrame(
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
