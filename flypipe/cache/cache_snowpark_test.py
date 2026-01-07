import os
from uuid import uuid4

import pandas as pd
import pytest

from flypipe.cache.cache import Cache
from flypipe.node import node


class GenericCacheSnowpark(Cache):
    def __init__(self):
        self.table_name = f"test_cache_{str(uuid4()).replace('-', '_')}"

    def read(self, session, from_node=None, to_node=None, is_static=False):
        return session.table(self.table_name).to_pandas()

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
        # Convert pandas DataFrame to Snowpark DataFrame and save as table
        snowpark_df = session.create_dataframe(df)
        snowpark_df.write.mode("overwrite").save_as_table(self.table_name)

    def exists(self, session):
        # Use try-catch approach since catalog API requires snowflake.core which isn't in localtest
        try:
            # First check if table can be accessed
            return session.table(self.table_name).count() > 0
            return True
        except Exception:
            return False


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") != "SNOWFLAKE",
    reason="Snowpark tests require RUN_MODE=SNOWFLAKE",
)
class TestCacheSnowpark:
    """Unit tests on the Cache class - Snowpark functionality"""

    def test_cache_with_session(self, snowflake_session, mocker):
        cache = GenericCacheSnowpark()

        @node(type="pandas", cache=cache, session_context=True)
        def t1(session):
            return pd.DataFrame(
                {
                    "c0": [0],
                    "c1": [1],
                }
            )

        spy_writter = mocker.spy(cache, "write")
        spy_reader = mocker.spy(cache, "read")
        spy_exists = mocker.spy(cache, "exists")
        t1.run(snowflake_session)
        assert spy_writter.call_count == 1
        assert spy_reader.call_count == 0
        assert spy_exists.call_count == 1

        spy_writter.reset_mock()
        spy_reader.reset_mock()
        spy_exists.reset_mock()
        t1.run(snowflake_session)
        assert spy_writter.call_count == 0
        assert spy_reader.call_count == 1
        assert spy_exists.call_count == 1
