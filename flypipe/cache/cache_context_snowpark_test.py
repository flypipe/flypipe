import os
from uuid import uuid4

import pandas as pd
import pytest

from flypipe.cache import CacheMode
from flypipe.cache.cache import Cache
from flypipe.cache.cache_context import CacheContext


class GenericCache(Cache):
    """Generic Pandas-based cache for testing"""
    def __init__(self):
        self.cache_csv = f"{str(uuid4())}.csv"

    def read(self, from_node=None, to_node=None, is_static=False):
        return pd.read_csv(self.cache_csv)

    def write(
        self,
        df,
        upstream_nodes=None,
        to_node=None,
        datetime_started_transformation=None,
    ):
        df.to_csv(self.cache_csv, index=False)

    def exists(self):
        return os.path.exists(self.cache_csv)


class GenericCacheSnowpark(Cache):
    """Generic Snowpark-compatible cache for testing"""
    def __init__(self):
        self.table_name = f"test_cache_{str(uuid4()).replace('-', '_')}"

    def read(self, session, from_node=None, to_node=None, is_static=False):
        return session.table(self.table_name).to_pandas()

    def write(
        self,
        session,
        df,
        upstream_nodes=None,
        to_node=None,
        datetime_started_transformation=None,
    ):
        # Convert pandas DataFrame to Snowpark DataFrame and save as table
        snowpark_df = session.create_dataframe(df)
        snowpark_df.write.mode("overwrite").save_as_table(self.table_name)

    def exists(self, session):
        # Use try-catch approach since catalog API requires snowflake.core which isn't in localtest
        try:
            # First check if table can be accessed
            return session.table(self.table_name).count() > 0
        except Exception:
            return False


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") != "SNOWFLAKE",
    reason="Snowpark tests require RUN_MODE=SNOWFLAKE",
)
class TestCacheContextSnowpark:
    """Unit tests on the CacheContext class - Snowpark functionality"""

    def test_write_read_with_session(self, snowflake_session):
        """Test write/read/exists operations with Snowpark cache"""
        cache_context = CacheContext(session=snowflake_session, cache=GenericCacheSnowpark())
        cache_context.write(pd.DataFrame(data={"col1": [1]}))
        cache_context.read()
        cache_context.exists()

    def test_write_incompatible_cache(self, snowflake_session):
        """Test that using non-Snowpark cache with Snowpark session raises error"""
        cache_context = CacheContext(session=snowflake_session, cache=GenericCache())

        with pytest.raises(TypeError):
            cache_context.write(pd.DataFrame(data={"col1": [1]}))

    def test_exists_with_session(self, snowflake_session):
        """Test exists() method with Snowpark cache"""
        cache_context = CacheContext(session=snowflake_session, cache=GenericCacheSnowpark())
        cache_context.exists()

    def test_exists_incompatible_cache(self, snowflake_session):
        """Test that exists() raises error when using non-Snowpark cache with Snowpark session"""
        cache_context = CacheContext(session=snowflake_session, cache=GenericCache())
        with pytest.raises(TypeError):
            cache_context.exists()

    def test_exists_session_cache_no_session(self):
        """Test that exists() raises error when Snowpark cache has no session"""
        cache_context = CacheContext(cache=GenericCacheSnowpark())
        with pytest.raises(TypeError):
            cache_context.exists()

