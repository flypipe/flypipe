import os
from uuid import uuid4

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from flypipe.cache.cdc_cache import CDCCache


class GenericCDCCache(CDCCache):
    """Generic CDC Cache implementation for testing purposes"""

    def __init__(self):
        self.cache_name = str(uuid4()).replace("-", "_")
        self.cache_csv = f"{self.cache_name}.csv"
        self.cdc_csv = f"{self.cache_name}_cdc.csv"

    def read(self):
        return pd.read_csv(self.cache_csv)

    def write(self, df):
        df.to_csv(self.cache_csv, index=False)

    def exists(self):
        return os.path.exists(self.cache_csv)

    def read_cdc(self):
        """Read CDC data from the CDC cache"""
        if os.path.exists(self.cdc_csv):
            return pd.read_csv(self.cdc_csv)
        return pd.DataFrame()

    def write_cdc(self, df):
        """Write CDC data to the CDC cache"""
        df.to_csv(self.cdc_csv, index=False)

    def create_cdc_table(self, *args, **kwargs):
        """Ensure CDC table/file structure exists (no-op for CSV-based cache)"""
        pass


@pytest.fixture(scope="function")
def cdc_cache():
    cache = GenericCDCCache()
    yield cache
    # Cleanup after test
    if os.path.exists(cache.cache_csv):
        os.remove(cache.cache_csv)
    if os.path.exists(cache.cdc_csv):
        os.remove(cache.cdc_csv)


class TestCDCCache:
    """Unit tests on the CDCCache class"""

    def test_cdc_cache_inheritance(self):
        """Test that CDCCache requires implementation of abstract methods"""

        class IncompleteCDCCache(CDCCache):
            pass

        with pytest.raises(TypeError):
            IncompleteCDCCache()

    def test_cdc_cache_missing_cdc_methods(self):
        """Test that CDCCache requires CDC-specific methods to be implemented"""

        class MissingCDCMethods(CDCCache):
            def read(self):
                pass

            def write(self, df):
                pass

            def exists(self):
                pass

        with pytest.raises(TypeError):
            MissingCDCMethods()

    def test_cdc_cache_complete_implementation(self, cdc_cache):
        """Test that a complete CDCCache implementation can be instantiated"""
        assert isinstance(cdc_cache, CDCCache)
        assert hasattr(cdc_cache, "read")
        assert hasattr(cdc_cache, "write")
        assert hasattr(cdc_cache, "exists")
        assert hasattr(cdc_cache, "read_cdc")
        assert hasattr(cdc_cache, "write_cdc")
        assert hasattr(cdc_cache, "create_cdc_table")

    def test_cdc_cache_write_read(self, cdc_cache):
        """Test basic write and read operations for regular cache"""
        test_df = pd.DataFrame(data={"col1": [1, 2, 3], "col2": [4, 5, 6]})

        assert not cdc_cache.exists()
        cdc_cache.write(test_df)
        assert cdc_cache.exists()

        result_df = cdc_cache.read()
        assert_frame_equal(test_df, result_df)

    def test_cdc_cache_write_cdc_read_cdc(self, cdc_cache):
        """Test CDC-specific write and read operations"""
        cdc_df = pd.DataFrame(
            data={
                "id": [1, 2, 3],
                "operation": ["insert", "update", "delete"],
                "timestamp": ["2023-01-01", "2023-01-02", "2023-01-03"],
            }
        )

        # Initially, no CDC data exists
        result = cdc_cache.read_cdc()
        assert result.empty

        # Write CDC data
        cdc_cache.write_cdc(cdc_df)

        # Read CDC data back
        result_df = cdc_cache.read_cdc()
        assert_frame_equal(cdc_df, result_df)

    def test_cdc_cache_separate_storage(self, cdc_cache):
        """Test that regular cache and CDC cache are stored separately"""
        regular_df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
        cdc_df = pd.DataFrame(data={"id": [1], "operation": ["insert"]})

        # Write to both caches
        cdc_cache.write(regular_df)
        cdc_cache.write_cdc(cdc_df)

        # Verify they are independent
        regular_result = cdc_cache.read()
        cdc_result = cdc_cache.read_cdc()

        assert_frame_equal(regular_df, regular_result)
        assert_frame_equal(cdc_df, cdc_result)
        assert not regular_result.equals(cdc_result)

    def test_cdc_cache_multiple_writes(self, cdc_cache):
        """Test multiple writes to CDC cache (overwrite behavior)"""
        cdc_df1 = pd.DataFrame(data={"id": [1], "operation": ["insert"]})
        cdc_df2 = pd.DataFrame(data={"id": [2], "operation": ["update"]})

        cdc_cache.write_cdc(cdc_df1)
        result1 = cdc_cache.read_cdc()
        assert_frame_equal(cdc_df1, result1)

        # Second write overwrites
        cdc_cache.write_cdc(cdc_df2)
        result2 = cdc_cache.read_cdc()
        assert_frame_equal(cdc_df2, result2)

    def test_cdc_cache_read_empty_cdc(self, cdc_cache):
        """Test reading CDC data when none exists"""
        result = cdc_cache.read_cdc()
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_cdc_cache_with_mocker(self, cdc_cache, mocker):
        """Test that CDC cache methods are called correctly"""
        spy_read_cdc = mocker.spy(cdc_cache, "read_cdc")
        spy_write_cdc = mocker.spy(cdc_cache, "write_cdc")

        cdc_df = pd.DataFrame(data={"id": [1], "operation": ["insert"]})

        # Write CDC data
        cdc_cache.write_cdc(cdc_df)
        assert spy_write_cdc.call_count == 1

        # Read CDC data
        cdc_cache.read_cdc()
        assert spy_read_cdc.call_count == 1

    def test_cdc_cache_independent_operations(self, cdc_cache, mocker):
        """Test that regular cache and CDC cache operations are independent"""
        regular_df = pd.DataFrame(data={"col1": [1]})
        cdc_df = pd.DataFrame(data={"id": [1], "operation": ["insert"]})

        spy_read = mocker.spy(cdc_cache, "read")
        spy_write = mocker.spy(cdc_cache, "write")
        spy_read_cdc = mocker.spy(cdc_cache, "read_cdc")
        spy_write_cdc = mocker.spy(cdc_cache, "write_cdc")

        # Write to regular cache
        cdc_cache.write(regular_df)
        assert spy_write.call_count == 1
        assert spy_write_cdc.call_count == 0

        # Write to CDC cache
        cdc_cache.write_cdc(cdc_df)
        assert spy_write.call_count == 1
        assert spy_write_cdc.call_count == 1

        # Read from regular cache
        cdc_cache.read()
        assert spy_read.call_count == 1
        assert spy_read_cdc.call_count == 0

        # Read from CDC cache
        cdc_cache.read_cdc()
        assert spy_read.call_count == 1
        assert spy_read_cdc.call_count == 1

    def test_cdc_cache_custom_implementation(self):
        """Test custom CDC cache implementation with different behavior"""

        class CustomCDCCache(CDCCache):
            def __init__(self):
                self.data = None
                self.cdc_data = []

            def read(self):
                return self.data

            def write(self, df):
                self.data = df

            def exists(self):
                return self.data is not None

            def read_cdc(self):
                """Returns all CDC entries as a concatenated DataFrame"""
                if not self.cdc_data:
                    return pd.DataFrame()
                return pd.concat(self.cdc_data, ignore_index=True)

            def write_cdc(self, df):
                """Appends CDC entries instead of overwriting"""
                self.cdc_data.append(df)

            def create_cdc_table(self, *args, **kwargs):
                """Ensure CDC table/file structure exists (no-op for in-memory cache)"""
                pass

        cache = CustomCDCCache()

        # Test append behavior
        cdc1 = pd.DataFrame(data={"id": [1], "operation": ["insert"]})
        cdc2 = pd.DataFrame(data={"id": [2], "operation": ["update"]})

        cache.write_cdc(cdc1)
        cache.write_cdc(cdc2)

        result = cache.read_cdc()
        expected = pd.DataFrame(data={"id": [1, 2], "operation": ["insert", "update"]})
        assert_frame_equal(result, expected)
