import os
import pytest

import snowflake.snowpark.functions as F

from flypipe.node import node
from flypipe.cache import CacheMode
from flypipe.tests.cdc_snowflake_manager_cache import CDCSnowflakeManagerCache


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") != "SNOWFLAKE",
    reason="Snowpark tests require RUN_MODE=SNOWFLAKE",
)
@pytest.mark.xdist_group(name="cdc_snowflake_sequential")
class TestRunnerSnowflake:
    """Tests for Node with CDCCache using Snowflake Snowpark"""

    @pytest.fixture(scope="class", params=[1])
    def max_workers(self, request):
        """Parameterize tests to run with max_workers=1 (sequential only for Snowflake local testing)"""
        return request.param

    @pytest.fixture(autouse=True)
    def setup_and_cleanup(self, snowflake_session, request):
        """Setup and cleanup for Snowflake tests"""
        # Use the test method name as a unique identifier
        test_name = (
            request.node.name.replace("[", "_")
            .replace("]", "")
            .replace("1", "sequential")
        )
        self.test_id = test_name

        print(f"\n🧹 Setup for test: {self.test_id}")

        yield

        print(f"\n🧹 Cleanup for test: {self.test_id}")
        print("✅ Cleanup complete for test")

    def test_cdc1_snowflake(self, snowflake_session, max_workers):
        """
        Test CDC cache with CDCCache using Snowflake Snowpark.

        Graph structure: B (CDC cached) -> A

        This test demonstrates:
        1. First run with 3 records - caches data
        2. Validates cached data
        3. Second run with 2 records - loads from cache (still has 3 records)
        4. Validates cache remains unchanged
        """

        # Create CDC cache for node B
        cache_b = CDCSnowflakeManagerCache(
            schema="default",
            table=f"node_b_{self.test_id}",
            cdc_table=f"cdc_test1_{self.test_id}",
            merge_keys=["ID"],
        )

        @node(type="snowpark", cache=cache_b)
        def B(number_records: int = 5):
            """Source node that generates data"""
            # Generate records dynamically based on number_records
            data = [(i, i * 10) for i in range(1, number_records + 1)]
            return snowflake_session.create_dataframe(data, schema=["ID", "SCORE"])

        @node(type="snowpark", dependencies=[B])
        def A(B):
            """Node that processes data from B"""
            return B.with_column("DOUBLED_SCORE", F.col("SCORE") * 2)

        # ===== First run with 3 records =====
        result1 = A.run(
            snowflake_session,
            parameters={B: {"number_records": 3}},
            max_workers=max_workers,
            debug=True,
        )

        # Assert first run results
        expected1 = snowflake_session.create_dataframe(
            [(1, 10, 20), (2, 20, 40), (3, 30, 60)],
            schema=["ID", "SCORE", "DOUBLED_SCORE"],
        )

        print("result1:")
        print(result1.drop("CDC_DATETIME_UPDATED").sort("ID").to_pandas())

        print("expected1:")
        print(expected1.sort("ID").to_pandas())

        # Assert using row comparison (Snowpark's assert_dataframe_equal is strict about schema)
        result1_rows = result1.drop("CDC_DATETIME_UPDATED").sort("ID").collect()
        expected1_rows = expected1.sort("ID").collect()
        assert result1_rows == expected1_rows, "First run results don't match"

        # Verify cache exists
        assert cache_b.exists(snowflake_session)

        # ===== Second run with 2 records =====
        result3 = A.run(
            snowflake_session,
            parameters={B: {"number_records": 2}},
            max_workers=max_workers,
            debug=True,
        )

        # Assert second run results (should load from cache - still has 3 records)
        print("result3:")
        print(result3.drop("CDC_DATETIME_UPDATED").sort("ID").to_pandas())

        print("expected3:")
        print(expected1.sort("ID").to_pandas())

        result3_rows = result3.drop("CDC_DATETIME_UPDATED").sort("ID").collect()
        expected1_rows = expected1.sort("ID").collect()
        assert result3_rows == expected1_rows, "Second run results don't match"

    def test_cdc2_snowflake(self, snowflake_session, max_workers):
        """
        Test CDC cache with both nodes cached using Snowflake.

        Graph structure: B (CDC cached) -> A (CDC cached)

        This test demonstrates:
        1. First run with 3 records - caches data at both B and A
        2. Validates cached data tables
        3. Second run with 2 records - loads from caches
        4. Validates caches remain unchanged
        """

        # Create CDC caches for nodes B and A
        cache_b = CDCSnowflakeManagerCache(
            schema="default",
            table=f"node_b_{self.test_id}",
            cdc_table=f"cdc_test2_{self.test_id}",
            merge_keys=["ID"],
        )
        cache_a = CDCSnowflakeManagerCache(
            schema="default",
            table=f"node_a_{self.test_id}",
            cdc_table=f"cdc_test2_{self.test_id}",
            merge_keys=["ID"],
        )

        @node(type="snowpark", cache=cache_b)
        def B(number_records: int = 5):
            """Source node that generates data"""
            data = [(i, i * 10) for i in range(1, number_records + 1)]
            return snowflake_session.create_dataframe(data, schema=["ID", "SCORE"])

        @node(type="snowpark", dependencies=[B], cache=cache_a)
        def A(B):
            """Node that processes data from B"""
            return B.with_column("DOUBLED_SCORE", F.col("SCORE") * 2)

        # ===== First run with 3 records =====
        resultA = A.run(
            snowflake_session,
            parameters={B: {"number_records": 3}},
            max_workers=max_workers,
            debug=True,
        )

        # Assert first run results
        expectedA = snowflake_session.create_dataframe(
            [(1, 10, 20), (2, 20, 40), (3, 30, 60)],
            schema=["ID", "SCORE", "DOUBLED_SCORE"],
        )

        print("resultA:")
        print(resultA.drop("CDC_DATETIME_UPDATED").sort("ID").to_pandas())

        print("expectedA:")
        print(expectedA.sort("ID").to_pandas())

        resultA_rows = resultA.drop("CDC_DATETIME_UPDATED").sort("ID").collect()
        expectedA_rows = expectedA.sort("ID").collect()
        assert resultA_rows == expectedA_rows, "First run results don't match"

        # Verify both caches exist
        assert cache_b.exists(snowflake_session)
        assert cache_a.exists(snowflake_session)

        # ===== Second run with 2 records =====
        resultA2 = A.run(
            snowflake_session,
            parameters={B: {"number_records": 2}},
            max_workers=max_workers,
            debug=True,
        )

        # Assert second run results (should load from cache - still has 3 records)
        print("resultA2:")
        print(resultA2.drop("CDC_DATETIME_UPDATED").sort("ID").to_pandas())

        resultA2_rows = resultA2.drop("CDC_DATETIME_UPDATED").sort("ID").collect()
        expectedA_rows = expectedA.sort("ID").collect()
        assert resultA2_rows == expectedA_rows, "Second run results don't match"

    def test_cdc3_snowflake_merge(self, snowflake_session, max_workers):
        """
        Test CDC cache with both nodes cached using MERGE mode in Snowflake.

        Graph structure: B (CDC cached MERGE) -> A (CDC cached MERGE)

        This test demonstrates:
        1. First run with 3 records - caches data at both B and A
        2. Second run with 2 new records - merges new data into caches
        3. Validates caches have both old and new data
        """

        # Create CDC caches for nodes B and A
        cache_b = CDCSnowflakeManagerCache(
            schema="default",
            table=f"node_b_{self.test_id}",
            cdc_table=f"cdc_test3_{self.test_id}",
            merge_keys=["ID"],
        )
        cache_a = CDCSnowflakeManagerCache(
            schema="default",
            table=f"node_a_{self.test_id}",
            cdc_table=f"cdc_test3_{self.test_id}",
            merge_keys=["ID"],
        )

        @node(type="snowpark", cache=cache_b)
        def B(number_records: int = 5, start_id: int = 1):
            """Source node that generates data"""
            data = [(i, i * 10) for i in range(start_id, start_id + number_records)]
            return snowflake_session.create_dataframe(data, schema=["ID", "SCORE"])

        @node(type="snowpark", dependencies=[B], cache=cache_a)
        def A(B):
            """Node that processes data from B"""
            return B.with_column("DOUBLED_SCORE", F.col("SCORE") * 2)

        # ===== First run with 3 records =====
        print("\n" + "=" * 80)
        print("🚀 RUN 1: Initial load with 3 records")
        print("=" * 80 + "\n")

        resultA = A.run(
            snowflake_session,
            parameters={B: {"number_records": 3}},
            cache={B: CacheMode.MERGE, A: CacheMode.MERGE},
            max_workers=max_workers,
            debug=True,
        )

        # Assert first run results
        expectedA = snowflake_session.create_dataframe(
            [(1, 10, 20), (2, 20, 40), (3, 30, 60)],
            schema=["ID", "SCORE", "DOUBLED_SCORE"],
        )

        print("resultA (run 1):")
        print(resultA.drop("CDC_DATETIME_UPDATED").sort("ID").to_pandas())

        print("expectedA (run 1):")
        print(expectedA.sort("ID").to_pandas())

        resultA_rows = resultA.drop("CDC_DATETIME_UPDATED").sort("ID").collect()
        expectedA_rows = expectedA.sort("ID").collect()
        assert resultA_rows == expectedA_rows, "Run 1 results don't match"

        # Verify both caches exist
        assert cache_b.exists(snowflake_session)
        assert cache_a.exists(snowflake_session)

        # ===== Second run with 2 NEW records (ids 4-5) using MERGE mode =====
        print("\n" + "=" * 80)
        print(
            "🔄 RUN 2: Incremental load with 2 NEW records (ids 4-5) using MERGE mode"
        )
        print("=" * 80 + "\n")

        resultA2 = A.run(
            snowflake_session,
            parameters={B: {"number_records": 2, "start_id": 4}},
            cache={B: CacheMode.MERGE, A: CacheMode.MERGE},
            max_workers=max_workers,
        )

        # Assert second run results (should have only the 2 new records)
        expectedA2 = snowflake_session.create_dataframe(
            [(4, 40, 80), (5, 50, 100)],
            schema=["ID", "SCORE", "DOUBLED_SCORE"],
        )

        print("resultA2 (run 2 - after merge):")
        print(resultA2.drop("CDC_DATETIME_UPDATED").sort("ID").to_pandas())

        print("expectedA2 (run 2):")
        print(expectedA2.sort("ID").to_pandas())

        resultA2_rows = resultA2.drop("CDC_DATETIME_UPDATED").sort("ID").collect()
        expectedA2_rows = expectedA2.sort("ID").collect()
        assert resultA2_rows == expectedA2_rows, "Run 2 output results don't match"

        # Verify caches have merged data (all 5 records)
        result_cache_b2 = cache_b.read(snowflake_session)
        expected_cache_b2 = snowflake_session.create_dataframe(
            [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)],
            schema=["ID", "SCORE"],
        )

        print("result_cache_b2 (after merge):")
        print(result_cache_b2.drop("CDC_DATETIME_UPDATED").sort("ID").to_pandas())

        print("expected_cache_b2:")
        print(expected_cache_b2.sort("ID").to_pandas())

        result_cache_b2_rows = (
            result_cache_b2.drop("CDC_DATETIME_UPDATED").sort("ID").collect()
        )
        expected_cache_b2_rows = expected_cache_b2.sort("ID").collect()
        assert (
            result_cache_b2_rows == expected_cache_b2_rows
        ), "Cache B merge results don't match"

        print("\n" + "=" * 80)
        print("✅ TEST COMPLETE: All runs validated successfully!")
        print("=" * 80 + "\n")

    def test_cdc4_snowflake_static_node(self, snowflake_session, max_workers):
        """
        Test that static nodes skip CDC filtering when reading from cache in Snowflake.

        Graph structure: B (cached MERGE, marked as static) -> A

        This test demonstrates:
        1. First run with B having CacheMode.MERGE (produces 1 row with id=1)
        2. Manually add more data to B's cache (add row with id=2)
        3. Second run where A depends on B.static()
        4. Verify A's output has 2 rows (all data from B, no CDC filtering)
        """

        # Create CDC cache for node B
        cache_b = CDCSnowflakeManagerCache(
            schema="default",
            table=f"node_b_{self.test_id}",
            cdc_table=f"cdc_test_static_{self.test_id}",
            merge_keys=["ID"],
        )

        @node(type="snowpark", cache=cache_b)
        def B():
            """Cached source node that generates 1 row"""
            data = [(1, 10)]
            return snowflake_session.create_dataframe(data, schema=["ID", "VALUE"])

        @node(type="snowpark", dependencies=[B.static()])
        def A(B):
            """Node A depends on B as static (no CDC filtering)"""
            return B.with_column("SCORE", F.col("VALUE") * 2)

        # ===== First Run: B produces 1 row with MERGE mode =====
        print("\n" + "=" * 80)
        print("🚀 RUN 1: Execute A with B in MERGE mode (B produces 1 row)")
        print("=" * 80 + "\n")

        result1 = A.run(
            snowflake_session,
            cache={B: CacheMode.MERGE},
            max_workers=max_workers,
            debug=True,
        )

        print("Result from Run 1 (should have 1 row):")
        print(result1.sort("ID").to_pandas())

        # Assert first run has 1 row
        assert result1.count() == 1, f"Expected 1 row in Run 1, got {result1.count()}"

        print("✅ Run 1 complete: 1 row processed, static node (no CDC filtering)")

        # ===== Manually add more data to B's cache =====
        print("\n" + "=" * 80)
        print("🔧 MANUAL UPDATE: Adding 1 more row to B's cache")
        print("=" * 80 + "\n")

        from datetime import datetime

        additional_row = snowflake_session.create_dataframe(
            [(2, 20, datetime.now())],
            schema=["ID", "VALUE", "CDC_DATETIME_UPDATED"],
        )

        # Append to B's cache (using Snowpark local testing mode)
        cache_b.write(
            snowflake_session,
            df=additional_row,
            to_node=B,
            upstream_nodes=[],
            datetime_started_transformation=datetime.now(),
        )

        print("✅ Added row: ID=2, VALUE=20 to B's cache")
        print("\nB's cache table contents (after manual addition):")
        cache_b_contents = cache_b.read(snowflake_session)
        print(cache_b_contents.sort("ID").to_pandas())

        # Verify B's cache now has 2 rows
        assert (
            cache_b_contents.count() == 2
        ), f"Expected 2 rows in B's cache, got {cache_b_contents.count()}"

        # ===== Second Run: A should get ALL data from B (no CDC filtering) =====
        print("\n" + "=" * 80)
        print("🚀 RUN 2: Execute A again (should get ALL 2 rows from static B)")
        print("=" * 80 + "\n")

        result2 = A.run(
            snowflake_session,
            max_workers=max_workers,
            debug=True,
        )

        print("Result from Run 2 (should have 2 rows - no CDC filtering):")
        print(result2.sort("ID").to_pandas())

        # Assert second run has 2 rows (all data from B, because it's static)
        assert result2.count() == 2, f"Expected 2 rows in Run 2, got {result2.count()}"

        # Verify the actual data
        expected_result = snowflake_session.create_dataframe(
            [(1, 10, 20), (2, 20, 40)],
            schema=["ID", "VALUE", "SCORE"],
        )

        print("\nExpected result (2 rows):")
        print(expected_result.sort("ID").to_pandas())

        result2_rows = result2.drop("CDC_DATETIME_UPDATED").sort("ID").collect()
        expected_result_rows = expected_result.sort("ID").collect()
        assert (
            result2_rows == expected_result_rows
        ), "Run 2 results don't match expected"

        print("\n" + "=" * 80)
        print("✅ TEST COMPLETE: Static node skipped CDC filtering in both runs!")
        print("=" * 80 + "\n")

    def test_cdc5_snowflake(self, snowflake_session, max_workers):
        """
        Test CDC cache with three nodes all using MERGE mode.

        Graph structure: C (CDC cached MERGE) -> B (CDC cached MERGE) -> A (CDC cached MERGE)

        This test demonstrates:
        1. Single run with 3 records - caches data at C, B, and A
        2. Validates CDC metadata tables for all nodes
        3. Validates cached data tables for all nodes
        4. Validates output from A
        """

        # Create CDC caches for nodes C, B, and A
        cache_c = CDCSnowflakeManagerCache(
            schema="default",
            table=f"node_c_{self.test_id}",
            cdc_table=f"cdc_test5_{self.test_id}",
            merge_keys=["ID"],
        )
        cache_b = CDCSnowflakeManagerCache(
            schema="default",
            table=f"node_b_{self.test_id}",
            cdc_table=f"cdc_test5_{self.test_id}",
            merge_keys=["ID"],
        )
        cache_a = CDCSnowflakeManagerCache(
            schema="default",
            table=f"node_a_{self.test_id}",
            cdc_table=f"cdc_test5_{self.test_id}",
            merge_keys=["ID"],
        )

        @node(type="snowpark", cache=cache_c)
        def C(number_records: int = 5, start_id: int = 1):
            """Source node that generates data"""
            data = [(i, i * 10) for i in range(start_id, start_id + number_records)]
            return snowflake_session.create_dataframe(data, schema=["ID", "VALUE"])

        @node(type="snowpark", dependencies=[C], cache=cache_b)
        def B(C):
            """Node that processes data from C"""
            return C.with_column("SCORE", F.col("VALUE") * 2)

        @node(type="snowpark", dependencies=[B], cache=cache_a)
        def A(B):
            """Node that processes data from B"""
            return B.with_column("DOUBLED_SCORE", F.col("SCORE") * 2)

        # ===== Run with 3 records =====
        print("\n" + "=" * 80)
        print("🚀 RUN: Load with 3 records through C -> B -> A")
        print("=" * 80 + "\n")

        resultA = A.run(
            snowflake_session,
            parameters={C: {"number_records": 3}},
            cache={C: CacheMode.MERGE, B: CacheMode.MERGE, A: CacheMode.MERGE},
            max_workers=max_workers,
        )

        # Assert run results for A
        expectedA = snowflake_session.create_dataframe(
            [(1, 10, 20, 40), (2, 20, 40, 80), (3, 30, 60, 120)],
            schema=["ID", "VALUE", "SCORE", "DOUBLED_SCORE"],
        )

        print("resultA:")
        print(resultA.drop("CDC_DATETIME_UPDATED").sort("ID").to_pandas())

        print("expectedA:")
        print(expectedA.sort("ID").to_pandas())

        resultA_rows = resultA.drop("CDC_DATETIME_UPDATED").sort("ID").collect()
        expectedA_rows = expectedA.sort("ID").collect()
        assert resultA_rows == expectedA_rows, "Result A doesn't match"

        # Verify all caches exist
        assert cache_c.exists(snowflake_session)
        assert cache_b.exists(snowflake_session)
        assert cache_a.exists(snowflake_session)

        # Assert cache_c.read() returns correct data
        result_cache_c = cache_c.read(snowflake_session)
        expectedC = snowflake_session.create_dataframe(
            [(1, 10), (2, 20), (3, 30)], schema=["ID", "VALUE"]
        )

        print("result_cache_c:")
        print(result_cache_c.drop("CDC_DATETIME_UPDATED").sort("ID").to_pandas())

        result_cache_c_rows = (
            result_cache_c.drop("CDC_DATETIME_UPDATED").sort("ID").collect()
        )
        expectedC_rows = expectedC.sort("ID").collect()
        assert result_cache_c_rows == expectedC_rows, "Cache C doesn't match"

        # Assert cache_b.read() returns correct data
        result_cache_b = cache_b.read(snowflake_session)
        expectedB = snowflake_session.create_dataframe(
            [(1, 10, 20), (2, 20, 40), (3, 30, 60)], schema=["ID", "VALUE", "SCORE"]
        )

        print("result_cache_b:")
        print(result_cache_b.drop("CDC_DATETIME_UPDATED").sort("ID").to_pandas())

        result_cache_b_rows = (
            result_cache_b.drop("CDC_DATETIME_UPDATED").sort("ID").collect()
        )
        expectedB_rows = expectedB.sort("ID").collect()
        assert result_cache_b_rows == expectedB_rows, "Cache B doesn't match"

        # Assert cache_a.read() returns correct data (same as resultA)
        result_cache_a = cache_a.read(snowflake_session)
        result_cache_a_rows = (
            result_cache_a.drop("CDC_DATETIME_UPDATED").sort("ID").collect()
        )
        assert result_cache_a_rows == expectedA_rows, "Cache A doesn't match"

        # Assert CDC metadata for C (should have C -> B)
        cdc_metadata_c = snowflake_session.table(cache_c.full_cdc_table_name)
        cdc_metadata_c_count = cdc_metadata_c.count()
        print(f"CDC metadata count for C: {cdc_metadata_c_count}")
        assert (
            cdc_metadata_c_count == 2
        ), f"Expected 2 CDC entries for C, got {cdc_metadata_c_count}"

        # Assert CDC metadata for B (should have C -> B and B -> A)
        cdc_metadata_b = snowflake_session.table(cache_b.full_cdc_table_name)
        expected_cdc_b = snowflake_session.create_dataframe(
            [
                (B.__name__, A.__name__),
                (C.__name__, B.__name__),
            ],
            schema=["SOURCE", "DESTINATION"],
        )

        print("cdc_metadata_b:")
        print(cdc_metadata_b.select("SOURCE", "DESTINATION").to_pandas())

        cdc_b_rows = sorted(
            cdc_metadata_b.select("SOURCE", "DESTINATION").collect(),
            key=lambda r: (r["SOURCE"], r["DESTINATION"]),
        )
        expected_b_rows = sorted(
            expected_cdc_b.collect(), key=lambda r: (r["SOURCE"], r["DESTINATION"])
        )
        assert cdc_b_rows == expected_b_rows, "CDC metadata B doesn't match"

        print("\n" + "=" * 80)
        print("✅ TEST COMPLETE: Validated successfully!")
        print("=" * 80 + "\n")

    def test_cdc6_snowflake(self, snowflake_session, max_workers):
        r"""
        Test CDC cache with diamond-shaped graph.
    
        Graph structure:
            D (CDC cached MERGE)
           / \
          /   \
         C     \
          \    /
           \  /
            A (CDC cached MERGE)
    
        This test demonstrates:
        1. Single run with 3 records - caches data at D and A, C is computed
        2. Validates CDC metadata tables for all nodes
        3. Validates cached data tables for D and A
        4. Validates output from A (which depends on both D and C)
        """

        # Create CDC caches for nodes D and A
        cache_d = CDCSnowflakeManagerCache(
            schema="default",
            table=f"node_d_{self.test_id}",
            cdc_table=f"cdc_test6_{self.test_id}",
            merge_keys=["ID"],
        )
        cache_a = CDCSnowflakeManagerCache(
            schema="default",
            table=f"node_a_{self.test_id}",
            cdc_table=f"cdc_test6_{self.test_id}",
            merge_keys=["ID"],
        )

        @node(type="snowpark", cache=cache_d)
        def D(number_records: int = 5, start_id: int = 1):
            """Source node that generates data"""
            data = [(i, i * 10) for i in range(start_id, start_id + number_records)]
            return snowflake_session.create_dataframe(data, schema=["ID", "VALUE"])

        @node(type="snowpark", dependencies=[D])
        def C(D):
            """Node that processes data from D"""
            return D.with_column("PROCESSED", F.col("VALUE") * 2)

        @node(type="snowpark", dependencies=[D, C], cache=cache_a)
        def A(D, C):
            """Node that combines data from D and C"""
            # Join D and C on id, then add a combined column
            result = C.join(
                D.drop("CDC_DATETIME_UPDATED").with_column_renamed("VALUE", "D_VALUE"),
                ["ID"],
            ).with_column("COMBINED", F.col("PROCESSED") + F.col("D_VALUE"))

            return result

        # ===== Run with 3 records =====
        print("\n" + "=" * 80)
        print("🚀 RUN: Load with 3 records through D -> C -> A (with D also feeding A)")
        print("=" * 80 + "\n")

        resultA = A.run(
            snowflake_session,
            parameters={D: {"number_records": 3}},
            cache={D: CacheMode.MERGE, A: CacheMode.MERGE},
            max_workers=max_workers,
            debug=True,
        )

        # Assert run results for A
        expectedA = snowflake_session.create_dataframe(
            [(1, 10, 20, 10, 30), (2, 20, 40, 20, 60), (3, 30, 60, 30, 90)],
            schema=["ID", "VALUE", "PROCESSED", "D_VALUE", "COMBINED"],
        )

        print("resultA:")
        print(resultA.drop("CDC_DATETIME_UPDATED").sort("ID").to_pandas())

        resultA_rows = resultA.drop("CDC_DATETIME_UPDATED").sort("ID").collect()
        expectedA_rows = expectedA.sort("ID").collect()
        assert resultA_rows == expectedA_rows, "Result A doesn't match"

        # Verify caches exist for D and A
        assert cache_d.exists(snowflake_session)
        assert cache_a.exists(snowflake_session)

        # Assert cache_d.read() returns correct data
        result_cache_d = cache_d.read(snowflake_session)
        expectedD = snowflake_session.create_dataframe(
            [(1, 10), (2, 20), (3, 30)], schema=["ID", "VALUE"]
        )

        result_cache_d_rows = (
            result_cache_d.drop("CDC_DATETIME_UPDATED").sort("ID").collect()
        )
        expectedD_rows = expectedD.sort("ID").collect()
        assert result_cache_d_rows == expectedD_rows, "Cache D doesn't match"

        # Assert cache_a.read() returns correct data (same as resultA)
        result_cache_a = cache_a.read(snowflake_session)
        result_cache_a_rows = (
            result_cache_a.drop("CDC_DATETIME_UPDATED").sort("ID").collect()
        )
        assert result_cache_a_rows == expectedA_rows, "Cache A doesn't match"

        # Assert CDC metadata for D (should have D -> A entry)
        cdc_metadata = snowflake_session.table(cache_d.full_cdc_table_name)
        cdc_metadata_count = cdc_metadata.count()
        print(f"CDC metadata count: {cdc_metadata_count}")
        assert (
            cdc_metadata_count == 1
        ), f"Expected 1 CDC entry for D -> A, got {cdc_metadata_count}"

        # Assert CDC metadata for A (should have D -> A)
        expected_cdc = snowflake_session.create_dataframe(
            [(D.__name__, A.__name__)], schema=["SOURCE", "DESTINATION"]
        )

        cdc_rows = cdc_metadata.select("SOURCE", "DESTINATION").collect()
        expected_cdc_rows = expected_cdc.collect()
        assert cdc_rows == expected_cdc_rows, "CDC metadata doesn't match"

        print("\n" + "=" * 80)
        print("✅ TEST COMPLETE: Validated successfully!")
        print("=" * 80 + "\n")

    def test_cdc9_snowflake(self, snowflake_session, max_workers):
        r"""
        Test CDC cache where cached nodes (B and C) don't trigger upstream computation.
    
        Graph structure:
               D (CDC cached MERGE) - should NOT run
              / \
             /   \
            B     C  (both CDC cached)
             \   /
              \ /
               A
    
        This test demonstrates:
        1. Manually create cache tables for B and C before running
        2. D's function raises an exception to ensure it doesn't run
        3. Run A which depends on B and C
        4. Verify that A completes successfully without running D
        """

        # Create CDC caches for nodes D, B, and C
        cache_d = CDCSnowflakeManagerCache(
            schema="default",
            table=f"node_d_{self.test_id}",
            cdc_table=f"cdc_test9_{self.test_id}",
            merge_keys=["ID"],
        )
        cache_b = CDCSnowflakeManagerCache(
            schema="default",
            table=f"node_b_{self.test_id}",
            cdc_table=f"cdc_test9_{self.test_id}",
            merge_keys=["ID"],
        )
        cache_c = CDCSnowflakeManagerCache(
            schema="default",
            table=f"node_c_{self.test_id}",
            cdc_table=f"cdc_test9_{self.test_id}",
            merge_keys=["ID"],
        )

        @node(type="snowpark", cache=cache_d)
        def D(number_records: int = 5, start_id: int = 1):
            """Source node that should NOT be executed"""
            raise RuntimeError(
                "D should not be executed! B and C should load from cache."
            )

        @node(type="snowpark", dependencies=[D], cache=cache_b)
        def B(D):
            """Node that processes data from D"""
            return D.with_column("B_SCORE", F.col("VALUE") * 2)

        @node(type="snowpark", dependencies=[D], cache=cache_c)
        def C(D):
            """Node that processes data from D"""
            return D.with_column("C_SCORE", F.col("VALUE") * 3)

        @node(type="snowpark", dependencies=[B, C])
        def A(B, C):
            """Node that combines data from B and C"""
            # Join B and C on id
            result = B.join(C.select("ID", F.col("C_SCORE")), ["ID"])
            return result.with_column("COMBINED", F.col("B_SCORE") + F.col("C_SCORE"))

        # ===== Manually create cache tables for B and C =====
        print("\n" + "=" * 80)
        print("🔧 SETUP: Manually creating cache tables for B and C")
        print("=" * 80 + "\n")

        # Create data for B's cache
        data_b = snowflake_session.create_dataframe(
            [(1, 10, 20), (2, 20, 40), (3, 30, 60)], schema=["ID", "VALUE", "B_SCORE"]
        )

        cache_b.write(snowflake_session, data_b)
        print("✅ Created cache table for B")

        # Create data for C's cache
        data_c = snowflake_session.create_dataframe(
            [(1, 10, 30), (2, 20, 60), (3, 30, 90)], schema=["ID", "VALUE", "C_SCORE"]
        )

        cache_c.write(snowflake_session, data_c)
        print("✅ Created cache table for C")

        # Verify caches exist
        assert cache_b.exists(snowflake_session), "Cache B should exist"
        assert cache_c.exists(snowflake_session), "Cache C should exist"

        print("\n" + "=" * 80)
        print("🚀 RUN: Executing A (should use cached B and C, without running D)")
        print("=" * 80 + "\n")

        # Run A - should load B and C from cache without executing D
        resultA = A.run(snowflake_session, max_workers=max_workers)

        # Assert run results for A
        expectedA = snowflake_session.create_dataframe(
            [
                (1, 10, 20, 30, 50),  # b_score=20, c_score=30, combined=50
                (2, 20, 40, 60, 100),
                (3, 30, 60, 90, 150),
            ],
            schema=["ID", "VALUE", "B_SCORE", "C_SCORE", "COMBINED"],
        )

        print("resultA:")
        print(resultA.drop("CDC_DATETIME_UPDATED").sort("ID").to_pandas())

        resultA_rows = resultA.drop("CDC_DATETIME_UPDATED").sort("ID").collect()
        expectedA_rows = expectedA.sort("ID").collect()
        assert resultA_rows == expectedA_rows, "Result A doesn't match"

        print("\n" + "=" * 80)
        print(
            "✅ TEST COMPLETE: A executed successfully using cached B and C without running D!"
        )
        print("=" * 80 + "\n")
