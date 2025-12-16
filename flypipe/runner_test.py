import os
import shutil
from unittest.mock import patch

import pyspark.sql.functions as F
import pytest

from flypipe.node import node
from flypipe.cache import CacheMode
from flypipe.tests.cdc_manager_cache import CDCManagerCache
from flypipe.tests.pyspark_test import assert_pyspark_df_equal


@pytest.mark.skipif(
    os.environ.get("USE_SPARK_CONNECT") != "1",
    reason="Requires Spark Connect environment (USE_SPARK_CONNECT=1)",
)
@pytest.mark.xdist_group(name="cdc_sequential")
class TestRunner:
    """Tests for Node with CDCCache"""

    @pytest.fixture(scope="class", params=[1, 2])
    def max_workers(self, request):
        """Parameterize tests to run with max_workers=1 (sequential) and max_workers=2 (parallel)"""
        return request.param

    @pytest.fixture(autouse=True)
    def setup_and_cleanup(self, spark, request):
        """Create test schema before each test and cleanup after"""
        # Use the test method name as the schema name, sanitize for SQL
        # Replace brackets and values to make it SQL-safe
        test_name = (
            request.node.name.replace("[", "_")
            .replace("]", "")
            .replace("1", "sequential")  # max_workers=1
            .replace("2", "parallel")
        )  # max_workers=2
        self.test_schema = test_name

        # Create schema and clean up base folder
        base_path = "/shared/delta/tables"
        schema_path = f"{base_path}/{self.test_schema}"

        print(f"\n🧹 Dropping schema if exists: {self.test_schema}")
        spark.sql(f"DROP SCHEMA IF EXISTS {self.test_schema} CASCADE")

        print(f"🧹 Deleting base folder if exists: {schema_path}")
        if os.path.exists(schema_path):
            shutil.rmtree(schema_path)
            print(f"✅ Deleted folder: {schema_path}")
        else:
            print(f"ℹ️  Folder does not exist: {schema_path}")

        print(f"✅ Creating fresh schema: {self.test_schema}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.test_schema}")

        # Assert schema has no tables
        print(f"🔍 Verifying schema {self.test_schema} is empty")

        yield

        # Cleanup after test
        base_path = "/shared/delta/tables"
        schema_path = f"{base_path}/{self.test_schema}"

        print(f"\n🧹 Cleaning up: Dropping schema {self.test_schema}")
        spark.sql(f"DROP SCHEMA IF EXISTS {self.test_schema} CASCADE")
        print(f"✅ Schema {self.test_schema} dropped")

        print(f"🧹 Deleting base folder: {schema_path}")
        if os.path.exists(schema_path):
            shutil.rmtree(schema_path)
            print(f"✅ Deleted folder: {schema_path}")
        else:
            print(f"ℹ️  Folder does not exist: {schema_path}")

        print("✅ Cleanup complete for test")

    def test_cdc1(self, spark, max_workers):
        """
        Test CDC cache with CDCManagerCache using Spark Delta tables.

        Graph structure: B (CDC cached) -> A

        This test demonstrates:
        1. First run with 3 records - caches data
        2. Validates CDC metadata table is empty (B has no upstream dependencies)
        3. Validates cached data table
        4. Second run with 2 records - loads from cache (still has 3 records)
        5. Validates cache remains unchanged
        """

        # Create CDC cache for node B using Delta table in test schema
        cache_b = CDCManagerCache(
            schema=self.test_schema,
            table="node_b",
            cdc_table="cdc_test1",
            merge_keys=["id"],
        )

        @node(type="pyspark", cache=cache_b)
        def B(number_records: int = 5):
            """Source node that generates data"""
            # Generate records dynamically based on number_records
            data = [(i, i * 10) for i in range(1, number_records + 1)]
            return spark.createDataFrame(data, ["id", "score"])

        @node(type="pyspark", dependencies=[B])
        def A(B):
            """Node that processes data from B"""
            return B.withColumn("doubled_score", F.col("score") * 2)

        # ===== First run with 3 records =====
        result1 = A.run(
            spark,
            parameters={B: {"number_records": 3}},
            max_workers=max_workers,
            debug=True,
        )

        # Assert first run results
        expected1 = spark.createDataFrame(
            [(1, 10, 20), (2, 20, 40), (3, 30, 60)], ["id", "score", "doubled_score"]
        )
        print("result1:")
        result1.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)
        print("expected1:")
        expected1.orderBy("id").show(truncate=False)
        assert_pyspark_df_equal(
            result1.drop("cdc_datetime_updated").orderBy("id"), expected1.orderBy("id")
        )

        # Verify cache exists
        assert cache_b.exists(spark)

        # Assert cached data in node_b table
        result2 = spark.table(cache_b.full_table_name)

        expected2 = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["id", "score"])
        print("result2:")
        result2.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)
        print("expected2:")
        expected2.orderBy("id").show(truncate=False)
        assert_pyspark_df_equal(
            result2.drop("cdc_datetime_updated").orderBy("id"), expected2.orderBy("id")
        )

        # Assert CDC metadata table is empty (B has no upstream dependencies)
        if spark.catalog.tableExists(cache_b.full_cdc_table_name):
            cdc_metadata = spark.table(cache_b.full_cdc_table_name)
            assert (
                cdc_metadata.count() == 0
            ), "CDC metadata should be empty when node has no upstream dependencies"

        # ===== Second run with 2 records =====
        result3 = A.run(
            spark,
            parameters={B: {"number_records": 2}},
            max_workers=max_workers,
            debug=True,
        )

        # Assert second run results (should load from cache - still has 3 records)
        expected3 = spark.createDataFrame(
            [(1, 10, 20), (2, 20, 40), (3, 30, 60)], ["id", "score", "doubled_score"]
        )
        print("result3:")
        result3.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)
        print("expected3:")
        expected3.orderBy("id").show(truncate=False)
        assert_pyspark_df_equal(
            result3.drop("cdc_datetime_updated").orderBy("id"), expected3.orderBy("id")
        )

        # Verify cache still has same data
        result4 = spark.table(cache_b.full_table_name)
        print("result4:")
        result4.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)
        assert_pyspark_df_equal(
            result4.drop("cdc_datetime_updated").orderBy("id"), expected2.orderBy("id")
        )

    def test_cdc2(self, spark, max_workers):
        """
        Test CDC cache with both nodes cached.

        Graph structure: B (CDC cached) -> A (CDC cached)

        This test demonstrates:
        1. First run with 3 records - caches data at both B and A
        2. Validates CDC metadata table for both nodes
        3. Validates cached data tables
        4. Second run with 2 records - loads from caches
        5. Validates caches remain unchanged
        """

        # Create CDC caches for nodes B and A using Delta tables in test schema
        cache_b = CDCManagerCache(
            schema=self.test_schema,
            table="node_b",
            cdc_table="cdc_test2",
            merge_keys=["id"],
        )
        cache_a = CDCManagerCache(
            schema=self.test_schema,
            table="node_a",
            cdc_table="cdc_test2",
            merge_keys=["id"],
        )

        @node(type="pyspark", cache=cache_b)
        def B(number_records: int = 5):
            """Source node that generates data"""
            # Generate records dynamically based on number_records
            data = [(i, i * 10) for i in range(1, number_records + 1)]
            return spark.createDataFrame(data, ["id", "score"])

        @node(type="pyspark", dependencies=[B], cache=cache_a)
        def A(B):
            """Node that processes data from B"""
            return B.withColumn("doubled_score", F.col("score") * 2)

        # ===== First run with 3 records =====
        resultA = A.run(
            spark,
            parameters={B: {"number_records": 3}},
            max_workers=max_workers,
            debug=True,
        )

        # Assert first run results
        expectedA = spark.createDataFrame(
            [(1, 10, 20), (2, 20, 40), (3, 30, 60)], ["id", "score", "doubled_score"]
        )

        print("resultA:")
        resultA.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedA:")
        expectedA.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            resultA.drop("cdc_datetime_updated").orderBy("id"), expectedA.orderBy("id")
        )

        # Verify both caches exist
        assert cache_b.exists(spark)
        assert cache_a.exists(spark)

        # Assert cache_b.read() returns correct data
        result_cache_b = cache_b.read(spark)

        expected_cache_b = spark.createDataFrame(
            [(1, 10), (2, 20), (3, 30)], ["id", "score"]
        )

        print("result_cache_b (cache_b.read()):")
        result_cache_b.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expected_cache_b:")
        expected_cache_b.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            result_cache_b.drop("cdc_datetime_updated").orderBy("id"),
            expected_cache_b.orderBy("id"),
        )

        # Assert CDC metadata table has entry for B -> A
        cdc_metadata_b = spark.table(cache_b.full_cdc_table_name)

        # Create expected CDC metadata DataFrame
        expected_cdc = spark.createDataFrame(
            [(B.__name__, A.__name__)], ["source", "destination"]
        )

        print("cdc_metadata_b:")
        spark.table(cache_b.full_cdc_table_name).show(truncate=False)

        print("expected_cdc:")
        expected_cdc.show(truncate=False)

        assert_pyspark_df_equal(
            cdc_metadata_b.select("source", "destination").orderBy(
                "source", "destination"
            ),
            expected_cdc.orderBy("source", "destination"),
        )

        # ===== Second run with 2 records =====
        resultA = A.run(
            spark,
            parameters={B: {"number_records": 2}},
            max_workers=max_workers,
            debug=True,
        )

        # Assert second run results (should load from cache - still has 3 records)
        expectedA = spark.createDataFrame(
            [(1, 10, 20), (2, 20, 40), (3, 30, 60)], ["id", "score", "doubled_score"]
        )

        print("resultA:")
        resultA.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)
        print("expectedA:")
        expectedA.orderBy("id").show(truncate=False)
        assert_pyspark_df_equal(
            resultA.drop("cdc_datetime_updated").orderBy("id"), expectedA.orderBy("id")
        )

        # Verify caches still have same data
        result5 = spark.table(cache_b.full_table_name)
        print("result5 (cache_b after second run):")
        result5.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)
        assert_pyspark_df_equal(
            result5.drop("cdc_datetime_updated").orderBy("id"),
            expected_cache_b.orderBy("id"),
        )

        result6 = spark.table(cache_a.full_table_name)
        print("result6 (cache_a after second run):")
        result6.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)
        assert_pyspark_df_equal(
            result6.drop("cdc_datetime_updated").orderBy("id"), expectedA.orderBy("id")
        )

    def test_cdc3(self, spark, max_workers):
        """
        Test CDC cache with both nodes cached using MERGE mode.

        Graph structure: B (CDC cached MERGE) -> A (CDC cached MERGE)

        This test demonstrates:
        1. First run with 3 records - caches data at both B and A
        2. Validates CDC metadata table for both nodes
        3. Validates cached data tables
        4. Second run with 2 new records - merges new data into caches
        5. Validates caches have both old and new data
        """

        # Create CDC caches for nodes B and A using Delta tables in test schema
        cache_b = CDCManagerCache(
            schema=self.test_schema,
            table="node_b",
            cdc_table="cdc_test3",
            merge_keys=["id"],
        )
        cache_a = CDCManagerCache(
            schema=self.test_schema,
            table="node_a",
            cdc_table="cdc_test3",
            merge_keys=["id"],
        )

        @node(type="pyspark", cache=cache_b)
        def B(number_records: int = 5, start_id: int = 1):
            """Source node that generates data"""
            # Generate records dynamically based on number_records
            data = [(i, i * 10) for i in range(start_id, start_id + number_records)]
            return spark.createDataFrame(data, ["id", "score"])

        @node(type="pyspark", dependencies=[B], cache=cache_a)
        def A(B):
            """Node that processes data from B"""
            return B.withColumn("doubled_score", F.col("score") * 2)

        # ===== First run with 3 records =====
        print("\n" + "=" * 80)
        print("🚀 RUN 1: Initial load with 3 records")
        print("=" * 80 + "\n")

        resultA = A.run(
            spark,
            parameters={B: {"number_records": 3}},
            cache={B: CacheMode.MERGE, A: CacheMode.MERGE},
            max_workers=max_workers,
            debug=True,
        )

        # Assert first run results
        expectedA = spark.createDataFrame(
            [(1, 10, 20), (2, 20, 40), (3, 30, 60)], ["id", "score", "doubled_score"]
        )

        print("resultA (run 1):")
        resultA.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedA (run 1):")
        expectedA.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            resultA.drop("cdc_datetime_updated").orderBy("id"), expectedA.orderBy("id")
        )

        # Verify both caches exist
        assert cache_b.exists(spark)
        assert cache_a.exists(spark)

        # Assert cache_b.read() returns correct data
        result_cache_b = cache_b.read(spark)

        expected_cache_b = spark.createDataFrame(
            [(1, 10), (2, 20), (3, 30)], ["id", "score"]
        )

        assert_pyspark_df_equal(
            result_cache_b.drop("cdc_datetime_updated").orderBy("id"),
            expected_cache_b.orderBy("id"),
        )

        # Assert CDC metadata table has entry for B -> A
        cdc_metadata_b = spark.table(cache_b.full_cdc_table_name)

        # Create expected CDC metadata DataFrame
        expected_cdc = spark.createDataFrame(
            [(B.__name__, A.__name__)], ["source", "destination"]
        )

        print("cdc_metadata_b (run 1):")
        spark.table(cache_b.full_cdc_table_name).show(truncate=False)

        print("expected_cdc (run 1):")
        expected_cdc.show(truncate=False)

        assert_pyspark_df_equal(
            cdc_metadata_b.select("source", "destination").orderBy(
                "source", "destination"
            ),
            expected_cdc.orderBy("source", "destination"),
        )

        # ===== Second run with 2 NEW records (ids 4-5) using MERGE mode =====
        print("\n" + "=" * 80)
        print(
            "🔄 RUN 2: Incremental load with 2 NEW records (ids 4-5) using MERGE mode"
        )
        print("=" * 80 + "\n")

        print("====> cache_b.read(spark):")
        cache_b.read(spark).show()
        print("====> cache_a.read(spark):")
        cache_a.read(spark).show()

        resultA2 = A.run(
            spark,
            parameters={B: {"number_records": 2, "start_id": 4}},
            cache={B: CacheMode.MERGE, A: CacheMode.MERGE},
            max_workers=max_workers,
        )

        # Assert second run results (should have all 5 records now)
        expectedA2 = spark.createDataFrame(
            [(4, 40, 80), (5, 50, 100)], ["id", "score", "doubled_score"]
        )

        print("resultA2 (run 2 - after merge):")
        resultA2.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedA2 (run 2):")
        expectedA2.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            resultA2.drop("cdc_datetime_updated").orderBy("id"),
            expectedA2.orderBy("id"),
        )

        # Verify caches have merged data (all 5 records)
        result_cache_b2 = cache_b.read(spark)
        expected_cache_b2 = spark.createDataFrame(
            [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)], ["id", "score"]
        )

        print("result_cache_b2 (after merge):")
        result_cache_b2.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expected_cache_b2:")
        expected_cache_b2.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            result_cache_b2.drop("cdc_datetime_updated").orderBy("id"),
            expected_cache_b2.orderBy("id"),
        )

        result_cache_a2 = cache_a.read(spark)
        print("result_cache_a2 (after merge):")
        result_cache_a2.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        # Assert second run results (should have all 5 records now)
        expectedA2 = spark.createDataFrame(
            [(1, 10, 20), (2, 20, 40), (3, 30, 60), (4, 40, 80), (5, 50, 100)],
            ["id", "score", "doubled_score"],
        )

        print("expectedA2 (run 2):")
        expectedA2.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            result_cache_a2.drop("cdc_datetime_updated").orderBy("id"),
            expectedA2.orderBy("id"),
        )

        # ===== Third run: Manually add 2 rows to B's table, run A with MERGE mode only =====
        print("\n" + "=" * 80)
        print("📥 RUN 3: Provide 2 new rows as input to B + A runs with MERGE mode")
        print("=" * 80 + "\n")

        # Create 2 new rows to pass as input to B
        new_rows = spark.createDataFrame([(6, 60), (7, 70)], ["id", "score"])

        print("Providing 2 new rows as input to B")

        # Run A with MERGE mode only (not B), passing new rows to B
        # This will trigger CDC filtering since B will process new data
        resultA3 = A.run(
            spark,
            inputs={B: new_rows},
            cache={A: CacheMode.MERGE},
            max_workers=max_workers,
        )

        # Assert third run results - output should contain ONLY the 2 new rows (CDC filtering)
        expectedA3_output = spark.createDataFrame(
            [(6, 60, 120), (7, 70, 140)], ["id", "score", "doubled_score"]
        )

        print("resultA3 (run 3 - only new rows in output):")
        resultA3.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedA3_output (only 2 new rows):")
        expectedA3_output.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            resultA3.drop("cdc_datetime_updated").orderBy("id"),
            expectedA3_output.orderBy("id"),
        )

        # Assert cache table for A now has ALL 7 rows (merged)
        result_cache_a3 = cache_a.read(spark)
        expectedA3_cache = spark.createDataFrame(
            [
                (1, 10, 20),
                (2, 20, 40),
                (3, 30, 60),
                (4, 40, 80),
                (5, 50, 100),
                (6, 60, 120),
                (7, 70, 140),
            ],
            ["id", "score", "doubled_score"],
        )

        print("result_cache_a3 (A's cache after merge - all 7 rows):")
        result_cache_a3.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedA3_cache (all 7 rows):")
        expectedA3_cache.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            result_cache_a3.drop("cdc_datetime_updated").orderBy("id"),
            expectedA3_cache.orderBy("id"),
        )

        print("\n" + "=" * 80)
        print("✅ TEST COMPLETE: All 3 runs validated successfully!")
        print("=" * 80 + "\n")

    def test_cdc4(self, spark, max_workers):
        """
        Test CDC cache with B (MERGE mode) and A (no cache).

        Graph structure: B (CDC cached MERGE) -> A

        This test demonstrates:
        1. Single run with 3 records - caches data at B, A processes from B
        2. Validates CDC metadata table for B (should be empty - no upstream)
        3. Validates cached data table for B
        4. Validates output from A
        5. Verifies cache.write and cache.write_cdc are called only once
        """

        # Create CDC cache for node B using Delta table in test schema
        cache_b = CDCManagerCache(
            schema=self.test_schema,
            table="node_b",
            cdc_table="cdc_test4",
            merge_keys=["id"],
        )

        @node(type="pyspark", cache=cache_b)
        def B(number_records: int = 5, start_id: int = 1):
            """Source node that generates data"""
            # Generate records dynamically based on number_records
            data = [(i, i * 10) for i in range(start_id, start_id + number_records)]
            return spark.createDataFrame(data, ["id", "score"])

        @node(type="pyspark", dependencies=[B])
        def A(B):
            """Node that processes data from B"""
            return B.withColumn("doubled_score", F.col("score") * 2)

        # ===== Run with 3 records =====
        print("\n" + "=" * 80)
        print("🚀 RUN: Load with 3 records")
        print("=" * 80 + "\n")

        # Patch cache methods to track calls
        with patch.object(cache_b, "write", wraps=cache_b.write) as mock_write:

            resultA = A.run(
                spark,
                parameters={B: {"number_records": 3}},
                cache={B: CacheMode.MERGE},
                max_workers=max_workers,
            )

            # Assert cache.write was called exactly once
            assert (
                mock_write.call_count == 1
            ), f"Expected cache.write to be called once, but was called {mock_write.call_count} times"

        # Assert run results for A
        expectedA = spark.createDataFrame(
            [(1, 10, 20), (2, 20, 40), (3, 30, 60)], ["id", "score", "doubled_score"]
        )

        print("resultA:")
        resultA.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedA:")
        expectedA.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            resultA.drop("cdc_datetime_updated").orderBy("id"), expectedA.orderBy("id")
        )

        # Verify cache exists for B
        assert cache_b.exists(spark)

        # Assert cache_b.read() returns correct data
        result_cache_b = cache_b.read(spark)
        expectedB = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["id", "score"])

        print("result_cache_b:")
        result_cache_b.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedB:")
        expectedB.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            result_cache_b.drop("cdc_datetime_updated").orderBy("id"),
            expectedB.orderBy("id"),
        )

        # Assert CDC metadata table is empty (B has no upstream dependencies)
        cdc_metadata_count = spark.table(cache_b.full_cdc_table_name).count()
        assert (
            cdc_metadata_count == 0
        ), f"Expected 0 CDC entries for B (no upstream), got {cdc_metadata_count}"

        print("\n" + "=" * 80)
        print("✅ TEST COMPLETE: Validated successfully!")
        print("=" * 80 + "\n")

    def test_cdc5(self, spark, max_workers):
        """
        Test CDC cache with three nodes all using MERGE mode.

        Graph structure: C (CDC cached MERGE) -> B (CDC cached MERGE) -> A (CDC cached MERGE)

        This test demonstrates:
        1. Single run with 3 records - caches data at C, B, and A
        2. Validates CDC metadata tables for all nodes
        3. Validates cached data tables for all nodes
        4. Validates output from A
        """

        # Create CDC caches for nodes C, B, and A using Delta tables in test schema
        cache_c = CDCManagerCache(
            schema=self.test_schema,
            table="node_c",
            cdc_table="cdc_test5",
            merge_keys=["id"],
        )
        cache_b = CDCManagerCache(
            schema=self.test_schema,
            table="node_b",
            cdc_table="cdc_test5",
            merge_keys=["id"],
        )
        cache_a = CDCManagerCache(
            schema=self.test_schema,
            table="node_a",
            cdc_table="cdc_test5",
            merge_keys=["id"],
        )

        @node(type="pyspark", cache=cache_c)
        def C(number_records: int = 5, start_id: int = 1):
            """Source node that generates data"""
            # Generate records dynamically based on number_records
            data = [(i, i * 10) for i in range(start_id, start_id + number_records)]
            return spark.createDataFrame(data, ["id", "value"])

        @node(type="pyspark", dependencies=[C], cache=cache_b)
        def B(C):
            """Node that processes data from C"""
            return C.withColumn("score", F.col("value") * 2)

        @node(type="pyspark", dependencies=[B], cache=cache_a)
        def A(B):
            """Node that processes data from B"""
            return B.withColumn("doubled_score", F.col("score") * 2)

        # ===== Run with 3 records =====
        print("\n" + "=" * 80)
        print("🚀 RUN: Load with 3 records through C -> B -> A")
        print("=" * 80 + "\n")

        resultA = A.run(
            spark,
            parameters={C: {"number_records": 3}},
            cache={C: CacheMode.MERGE, B: CacheMode.MERGE, A: CacheMode.MERGE},
            max_workers=max_workers,
        )

        # Assert run results for A
        expectedA = spark.createDataFrame(
            [(1, 10, 20, 40), (2, 20, 40, 80), (3, 30, 60, 120)],
            ["id", "value", "score", "doubled_score"],
        )

        print("resultA:")
        resultA.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedA:")
        expectedA.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            resultA.drop("cdc_datetime_updated").orderBy("id"), expectedA.orderBy("id")
        )

        # Verify all caches exist
        assert cache_c.exists(spark)
        assert cache_b.exists(spark)
        assert cache_a.exists(spark)

        # Assert cache_c.read() returns correct data
        result_cache_c = cache_c.read(spark)
        expectedC = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["id", "value"])

        print("result_cache_c:")
        result_cache_c.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedC:")
        expectedC.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            result_cache_c.drop("cdc_datetime_updated").orderBy("id"),
            expectedC.orderBy("id"),
        )

        # Assert cache_b.read() returns correct data
        result_cache_b = cache_b.read(spark)
        expectedB = spark.createDataFrame(
            [(1, 10, 20), (2, 20, 40), (3, 30, 60)], ["id", "value", "score"]
        )

        print("result_cache_b:")
        result_cache_b.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedB:")
        expectedB.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            result_cache_b.drop("cdc_datetime_updated").orderBy("id"),
            expectedB.orderBy("id"),
        )

        # Assert cache_a.read() returns correct data (same as resultA)
        result_cache_a = cache_a.read(spark)
        assert_pyspark_df_equal(
            result_cache_a.drop("cdc_datetime_updated").orderBy("id"),
            expectedA.orderBy("id"),
        )

        # Assert CDC metadata for C (should be empty - no upstream)
        cdc_metadata_c = spark.table(cache_c.full_cdc_table_name)
        cdc_metadata_c_count = cdc_metadata_c.count()
        cdc_metadata_c.show(truncate=False)
        assert (
            cdc_metadata_c_count == 2
        ), f"Expected 2 CDC entries for C, got {cdc_metadata_c_count}"

        # Assert CDC metadata for B (should have C -> B and B -> A)
        cdc_metadata_b = spark.table(cache_b.full_cdc_table_name)
        expected_cdc_b = spark.createDataFrame(
            [
                (B.__name__, A.__name__),
                (C.__name__, B.__name__),
            ],
            ["source", "destination"],
        )

        print("cdc_metadata_b:")
        cdc_metadata_b.select("source", "destination").show(truncate=False)
        assert_pyspark_df_equal(
            cdc_metadata_b.select("source", "destination").orderBy(
                "source", "destination"
            ),
            expected_cdc_b.orderBy("source", "destination"),
        )

        print("\n" + "=" * 80)
        print("✅ TEST COMPLETE: Validated successfully!")
        print("=" * 80 + "\n")

    def test_cdc6(self, spark, max_workers):
        """
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

        # Create CDC caches for nodes D and A using Delta tables in test schema
        cache_d = CDCManagerCache(
            schema=self.test_schema,
            table="node_d",
            cdc_table="cdc_test6",
            merge_keys=["id"],
        )
        cache_a = CDCManagerCache(
            schema=self.test_schema,
            table="node_a",
            cdc_table="cdc_test6",
            merge_keys=["id"],
        )

        @node(type="pyspark", cache=cache_d)
        def D(number_records: int = 5, start_id: int = 1):
            """Source node that generates data"""
            # Generate records dynamically based on number_records
            data = [(i, i * 10) for i in range(start_id, start_id + number_records)]
            return spark.createDataFrame(data, ["id", "value"])

        @node(type="pyspark", dependencies=[D])
        def C(D):
            """Node that processes data from D"""
            return D.withColumn("processed", F.col("value") * 2)

        @node(type="pyspark", dependencies=[D, C], cache=cache_a)
        def A(D, C):
            """Node that combines data from D and C"""
            # Join D and C on id, then add a combined column
            C = C.join(
                D.drop("cdc_datetime_updated").withColumnRenamed("value", "d_value"),
                on="id",
                how="left",
            ).withColumn("combined", F.col("processed") + F.col("d_value"))

            return C

        # ===== Run with 3 records =====
        print("\n" + "=" * 80)
        print("🚀 RUN: Load with 3 records through D -> C -> A (with D also feeding A)")
        print("=" * 80 + "\n")

        resultA = A.run(
            spark,
            parameters={D: {"number_records": 3}},
            cache={D: CacheMode.MERGE, A: CacheMode.MERGE},
            max_workers=max_workers,
            debug=True,
        )

        # Assert run results for A
        expectedA = spark.createDataFrame(
            [(1, 10, 20, 10, 30), (2, 20, 40, 20, 60), (3, 30, 60, 30, 90)],
            ["id", "value", "processed", "d_value", "combined"],
        )

        print("resultA:")
        resultA.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedA:")
        expectedA.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            resultA.drop("cdc_datetime_updated").orderBy("id"), expectedA.orderBy("id")
        )

        # Verify caches exist for D and A
        assert cache_d.exists(spark)
        assert cache_a.exists(spark)

        # Assert cache_d.read() returns correct data
        result_cache_d = cache_d.read(spark)
        expectedD = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["id", "value"])

        print("result_cache_d:")
        result_cache_d.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedD:")
        expectedD.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            result_cache_d.drop("cdc_datetime_updated").orderBy("id"),
            expectedD.orderBy("id"),
        )

        # Assert cache_a.read() returns correct data (same as resultA)
        result_cache_a = cache_a.read(spark)
        assert_pyspark_df_equal(
            result_cache_a.drop("cdc_datetime_updated").orderBy("id"),
            expectedA.orderBy("id"),
        )

        # Assert CDC metadata for D (should have D -> A entry)
        cdc_metadata = spark.table(cache_d.full_cdc_table_name)
        cdc_metadata_count = cdc_metadata.count()
        print("cdc_metadata_d:")
        cdc_metadata.show(truncate=False)
        assert (
            cdc_metadata_count == 1
        ), f"Expected 1 CDC entry for D -> A, got {cdc_metadata_count}"

        # Assert CDC metadata for A (should have D -> A)
        # Note: C is not cached, so only D (the first cached predecessor) should be in CDC metadata
        expected_cdc = spark.createDataFrame(
            [(D.__name__, A.__name__)], ["source", "destination"]
        )

        assert_pyspark_df_equal(
            cdc_metadata.select("source", "destination").orderBy(
                "source", "destination"
            ),
            expected_cdc.orderBy("source", "destination"),
        )

        print("\n" + "=" * 80)
        print("✅ TEST COMPLETE: Validated successfully!")
        print("=" * 80 + "\n")

    def test_cdc7(self, spark, max_workers):
        """
        Test CDC cache with C and B cached (MERGE mode), A not cached.

        Graph structure: C (CDC cached MERGE) -> B (CDC cached MERGE) -> A

        This test demonstrates:
        1. Single run with 3 records - caches data at C and B, A processes from B
        2. Validates CDC metadata tables for C and B
        3. Validates cached data tables for C and B
        4. Validates output from A
        """

        # Create CDC caches for nodes C and B using Delta tables in test schema
        cache_c = CDCManagerCache(
            schema=self.test_schema,
            table="node_c",
            cdc_table="cdc_test7",
            merge_keys=["id"],
        )
        cache_b = CDCManagerCache(
            schema=self.test_schema,
            table="node_b",
            cdc_table="cdc_test7",
            merge_keys=["id"],
        )

        @node(type="pyspark", cache=cache_c)
        def C(number_records: int = 5, start_id: int = 1):
            """Source node that generates data"""
            # Generate records dynamically based on number_records
            data = [(i, i * 10) for i in range(start_id, start_id + number_records)]
            return spark.createDataFrame(data, ["id", "value"])

        @node(type="pyspark", dependencies=[C], cache=cache_b)
        def B(C):
            """Node that processes data from C"""
            return C.withColumn("score", F.col("value") * 2)

        @node(type="pyspark", dependencies=[B])
        def A(B):
            """Node that processes data from B"""
            return B.withColumn("doubled_score", F.col("score") * 2)

        # ===== Run with 3 records =====
        print("\n" + "=" * 80)
        print("🚀 RUN: Load with 3 records through C -> B -> A")
        print("=" * 80 + "\n")

        resultA = A.run(
            spark,
            parameters={C: {"number_records": 3}},
            cache={C: CacheMode.MERGE, B: CacheMode.MERGE},
            max_workers=max_workers,
        )

        # Assert run results for A
        expectedA = spark.createDataFrame(
            [(1, 10, 20, 40), (2, 20, 40, 80), (3, 30, 60, 120)],
            ["id", "value", "score", "doubled_score"],
        )

        print("resultA:")
        resultA.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedA:")
        expectedA.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            resultA.drop("cdc_datetime_updated").orderBy("id"), expectedA.orderBy("id")
        )

        # Verify caches exist for C and B
        assert cache_c.exists(spark)
        assert cache_b.exists(spark)

        # Assert cache_c.read() returns correct data
        result_cache_c = cache_c.read(spark)
        expectedC = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["id", "value"])

        print("result_cache_c:")
        result_cache_c.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedC:")
        expectedC.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            result_cache_c.drop("cdc_datetime_updated").orderBy("id"),
            expectedC.orderBy("id"),
        )

        # Assert cache_b.read() returns correct data
        result_cache_b = cache_b.read(spark)
        expectedB = spark.createDataFrame(
            [(1, 10, 20), (2, 20, 40), (3, 30, 60)], ["id", "value", "score"]
        )

        print("result_cache_b:")
        result_cache_b.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedB:")
        expectedB.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            result_cache_b.drop("cdc_datetime_updated").orderBy("id"),
            expectedB.orderBy("id"),
        )

        # Assert CDC metadata for B (should have C -> B)
        cdc_metadata_b = spark.table(cache_b.full_cdc_table_name)
        expected_cdc_b = spark.createDataFrame(
            [(C.__name__, B.__name__)], ["source", "destination"]
        )

        print("cdc_metadata_b:")
        cdc_metadata_b.select("source", "destination").show(truncate=False)

        assert_pyspark_df_equal(
            cdc_metadata_b.select("source", "destination").orderBy(
                "source", "destination"
            ),
            expected_cdc_b.orderBy("source", "destination"),
        )

        print("\n" + "=" * 80)
        print("✅ TEST COMPLETE: Validated successfully!")
        print("=" * 80 + "\n")

    def test_cdc8(self, spark, max_workers):
        """
        Test CDC cache with complex diamond graph.
    
        Graph structure:
               D (CDC cached MERGE)
              /|\
             / | \
            /  |  \
           C   B   (D also directly to A)
            \ / \ /
             \ /
              A
    
        Where:
        - D -> A (D feeds directly to A)
        - D -> B (D feeds to B, B is cached MERGE)
        - D -> C (D feeds to C, C is not cached)
        - B -> A (B also feeds to A)
        - C -> A (C also feeds to A)
    
        This test demonstrates:
        1. Single run with 3 records - caches data at D and B
        2. Validates CDC metadata tables for D and B
        3. Validates cached data tables for D and B
        4. Validates output from A (which depends on D, B, and C)
        """

        # Create CDC caches for nodes D and B using Delta tables in test schema
        cache_d = CDCManagerCache(
            schema=self.test_schema,
            table="node_d",
            cdc_table="cdc_test8",
            merge_keys=["id"],
        )
        cache_b = CDCManagerCache(
            schema=self.test_schema,
            table="node_b",
            cdc_table="cdc_test8",
            merge_keys=["id"],
        )

        @node(type="pyspark", cache=cache_d)
        def D(number_records: int = 5, start_id: int = 1):
            """Source node that generates data"""
            # Generate records dynamically based on number_records
            data = [(i, i * 10) for i in range(start_id, start_id + number_records)]
            return spark.createDataFrame(data, ["id", "value"])

        @node(type="pyspark", dependencies=[D], cache=cache_b)
        def B(D):
            """Node that processes data from D"""
            return D.withColumn("b_score", F.col("value") * 2)

        @node(type="pyspark", dependencies=[D])
        def C(D):
            """Node that processes data from D"""
            return D.withColumn("c_score", F.col("value") * 3)

        @node(type="pyspark", dependencies=[D, B, C])
        def A(D, B, C):
            """Node that combines data from D, B, and C"""
            # Join D, B, and C on id
            result = C.join(
                D.select("id", F.col("value").alias("d_value")), on="id", how="inner"
            )
            result = result.join(B.select("id", F.col("b_score")), on="id", how="inner")
            # Add a_score column
            return result.withColumn("a_score", F.col("d_value") * 4)

        # ===== Run with 3 records =====
        print("\n" + "=" * 80)
        print("🚀 RUN: Load with 3 records through D -> (B, C, A)")
        print("=" * 80 + "\n")

        resultA = A.run(
            spark,
            parameters={D: {"number_records": 3}},
            cache={D: CacheMode.MERGE, B: CacheMode.MERGE},
            max_workers=max_workers,
            debug=True,
        )

        # Assert run results for A
        # A should have: id, value, c_score (from C), d_value (from D), b_score (from B), a_score
        expectedA = spark.createDataFrame(
            [
                (
                    1,
                    10,
                    30,
                    10,
                    20,
                    40,
                ),  # c_score=30, d_value=10, b_score=20, a_score=40
                (2, 20, 60, 20, 40, 80),
                (3, 30, 90, 30, 60, 120),
            ],
            ["id", "value", "c_score", "d_value", "b_score", "a_score"],
        )

        print("resultA:")
        resultA.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedA:")
        expectedA.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            resultA.drop("cdc_datetime_updated").orderBy("id"), expectedA.orderBy("id")
        )

        # Verify caches exist for D and B
        assert cache_d.exists(spark)
        assert cache_b.exists(spark)

        # Assert cache_d.read() returns correct data
        result_cache_d = cache_d.read(spark)
        expectedD = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["id", "value"])

        print("result_cache_d:")
        result_cache_d.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedD:")
        expectedD.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            result_cache_d.drop("cdc_datetime_updated").orderBy("id"),
            expectedD.orderBy("id"),
        )

        # Assert cache_b.read() returns correct data
        result_cache_b = cache_b.read(spark)
        expectedB = spark.createDataFrame(
            [(1, 10, 20), (2, 20, 40), (3, 30, 60)], ["id", "value", "b_score"]
        )

        print("result_cache_b:")
        result_cache_b.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedB:")
        expectedB.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            result_cache_b.drop("cdc_datetime_updated").orderBy("id"),
            expectedB.orderBy("id"),
        )

        # Assert CDC metadata for B (should have D -> B)
        cdc_metadata_b = spark.table(cache_b.full_cdc_table_name)
        expected_cdc_b = spark.createDataFrame(
            [(D.__name__, B.__name__)], ["source", "destination"]
        )

        print("cdc_metadata_b:")
        cdc_metadata_b.select("source", "destination").show(truncate=False)

        assert_pyspark_df_equal(
            cdc_metadata_b.select("source", "destination").orderBy(
                "source", "destination"
            ),
            expected_cdc_b.orderBy("source", "destination"),
        )

        print("\n" + "=" * 80)
        print("✅ TEST COMPLETE: Validated successfully!")
        print("=" * 80 + "\n")

    def test_cdc9(self, spark, max_workers):
        """
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

        # Create CDC caches for nodes D, B, and C using Delta tables in test schema
        cache_d = CDCManagerCache(
            schema=self.test_schema,
            table="node_d",
            cdc_table="cdc_test9",
            merge_keys=["id"],
        )
        cache_b = CDCManagerCache(
            schema=self.test_schema,
            table="node_b",
            cdc_table="cdc_test9",
            merge_keys=["id"],
        )
        cache_c = CDCManagerCache(
            schema=self.test_schema,
            table="node_c",
            cdc_table="cdc_test9",
            merge_keys=["id"],
        )

        @node(type="pyspark", cache=cache_d)
        def D(number_records: int = 5, start_id: int = 1):
            """Source node that should NOT be executed"""
            raise RuntimeError(
                "D should not be executed! B and C should load from cache."
            )

        @node(type="pyspark", dependencies=[D], cache=cache_b)
        def B(D):
            """Node that processes data from D"""
            return D.withColumn("b_score", F.col("value") * 2)

        @node(type="pyspark", dependencies=[D], cache=cache_c)
        def C(D):
            """Node that processes data from D"""
            return D.withColumn("c_score", F.col("value") * 3)

        @node(type="pyspark", dependencies=[B, C])
        def A(B, C):
            """Node that combines data from B and C"""
            # Join B and C on id
            result = B.join(C.select("id", F.col("c_score")), on="id", how="inner")
            return result.withColumn("combined", F.col("b_score") + F.col("c_score"))

        # ===== Manually create cache tables for B and C =====
        print("\n" + "=" * 80)
        print("🔧 SETUP: Manually creating cache tables for B and C")
        print("=" * 80 + "\n")

        # Create data for B's cache
        data_b = spark.createDataFrame(
            [(1, 10, 20), (2, 20, 40), (3, 30, 60)], ["id", "value", "b_score"]
        )

        cache_b.write(spark, data_b)
        print("✅ Created cache table for B")

        # Create data for C's cache
        data_c = spark.createDataFrame(
            [(1, 10, 30), (2, 20, 60), (3, 30, 90)], ["id", "value", "c_score"]
        )

        cache_c.write(spark, data_c)
        print("✅ Created cache table for C")

        # Verify caches exist
        assert cache_b.exists(spark), "Cache B should exist"
        assert cache_c.exists(spark), "Cache C should exist"

        print("\n" + "=" * 80)
        print("🚀 RUN: Executing A (should use cached B and C, without running D)")
        print("=" * 80 + "\n")

        # Run A - should load B and C from cache without executing D
        resultA = A.run(spark, max_workers=max_workers)

        # Assert run results for A
        expectedA = spark.createDataFrame(
            [
                (1, 10, 20, 30, 50),  # b_score=20, c_score=30, combined=50
                (2, 20, 40, 60, 100),
                (3, 30, 60, 90, 150),
            ],
            ["id", "value", "b_score", "c_score", "combined"],
        )

        print("resultA:")
        resultA.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedA:")
        expectedA.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            resultA.drop("cdc_datetime_updated").orderBy("id"), expectedA.orderBy("id")
        )

        # Verify B and C caches still have the original data
        result_cache_b = cache_b.read(spark)
        expected_cache_b = spark.createDataFrame(
            [(1, 10, 20), (2, 20, 40), (3, 30, 60)], ["id", "value", "b_score"]
        )

        print("result_cache_b:")
        result_cache_b.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            result_cache_b.drop("cdc_datetime_updated").orderBy("id"),
            expected_cache_b.orderBy("id"),
        )

        result_cache_c = cache_c.read(spark)
        expected_cache_c = spark.createDataFrame(
            [(1, 10, 30), (2, 20, 60), (3, 30, 90)], ["id", "value", "c_score"]
        )

        print("result_cache_c:")
        result_cache_c.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            result_cache_c.drop("cdc_datetime_updated").orderBy("id"),
            expected_cache_c.orderBy("id"),
        )

        print("\n" + "=" * 80)
        print(
            "✅ TEST COMPLETE: A executed successfully using cached B and C without running D!"
        )
        print("=" * 80 + "\n")

    def test_cdc10(self, spark, max_workers):
        """
        Test CDC timestamp filtering with manually inserted CDC metadata.

        Graph structure: B (CDC cached MERGE) -> A (CDC cached MERGE)

        This test demonstrates:
        1. Node B returns 2 rows with different cdc_datetime_updated timestamps
        2. Manually insert CDC metadata with the earlier timestamp
        3. Run A with MERGE mode
        4. Verify that only the row with the later timestamp is processed (CDC filtering)
        """
        from datetime import datetime, timedelta

        # Create CDC caches for nodes B and A using Delta tables in test schema
        cache_b = CDCManagerCache(
            schema=self.test_schema,
            table="node_b",
            cdc_table="cdc_test10",
            merge_keys=["id"],
        )
        cache_a = CDCManagerCache(
            schema=self.test_schema,
            table="node_a",
            cdc_table="cdc_test10",
            merge_keys=["id"],
        )

        # Define timestamps
        dt = datetime(2025, 1, 1)
        dt_plus_one = dt + timedelta(seconds=1)

        @node(type="pyspark", cache=cache_b)
        def B():
            """Source node that generates data with specific timestamps"""
            data = [(1, 10, dt), (2, 20, dt_plus_one)]
            return spark.createDataFrame(data, ["id", "score", "cdc_datetime_updated"])

        @node(type="pyspark", dependencies=[B], cache=cache_a)
        def A(B):
            """Node that processes data from B"""
            return B.withColumn("doubled_score", F.col("score") * 2)

        # ===== Manually insert CDC metadata row =====
        print("\n" + "=" * 80)
        print("🔧 SETUP: Manually inserting CDC metadata")
        print("=" * 80 + "\n")

        # Ensure CDC metadata table exists
        cache_b.create_cdc_table(spark)

        # Insert CDC metadata: source=B, destination=A, timestamp=dt
        cdc_metadata_row = spark.createDataFrame(
            [(B.__name__, A.__name__, dt)],
            ["source", "destination", "cdc_datetime_updated"],
        )

        # Write to CDC metadata table
        cdc_metadata_row.write.format("delta").mode("append").partitionBy(
            "source", "destination"
        ).save(cache_b.cdc_table_path)

        print(
            f"✅ Inserted CDC metadata: source={B.__name__}, destination={A.__name__}, timestamp={dt}"
        )
        print("\nCDC metadata table contents:")
        spark.table(cache_b.full_cdc_table_name).show(truncate=False)

        # ===== Run A with MERGE mode =====
        print("\n" + "=" * 80)
        print(
            "🚀 RUN: Executing A with MERGE mode (should filter based on CDC timestamp)"
        )
        print("=" * 80 + "\n")

        resultA = A.run(
            spark,
            cache={B: CacheMode.MERGE, A: CacheMode.MERGE},
            max_workers=max_workers,
            debug=True,
        )

        # Assert run results for A - should only contain row with dt_plus_one (id=2)
        expectedA = spark.createDataFrame(
            [(2, 20, dt_plus_one, 40)],
            ["id", "score", "cdc_datetime_updated", "doubled_score"],
        )

        print("resultA (should only have 1 row with dt_plus_one):")
        resultA.orderBy("id").show(truncate=False)

        print("expectedA (only row with dt_plus_one):")
        expectedA.orderBy("id").show(truncate=False)

        # Assert only 1 row is returned
        assert (
            resultA.count() == 1
        ), f"Expected 1 row in output, but got {resultA.count()}"

        # Assert the row has the correct values
        assert_pyspark_df_equal(resultA.orderBy("id"), expectedA.orderBy("id"))

        # Verify cache_b has both rows
        result_cache_b = cache_b.read(spark)
        expected_cache_b = spark.createDataFrame(
            [(1, 10, dt), (2, 20, dt_plus_one)], ["id", "score", "cdc_datetime_updated"]
        )

        print("result_cache_b (should have both rows):")
        result_cache_b.orderBy("id").show(truncate=False)

        print("expected_cache_b:")
        expected_cache_b.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            result_cache_b.orderBy("id"), expected_cache_b.orderBy("id")
        )

        # Verify cache_a has only the filtered row
        result_cache_a = cache_a.read(spark)

        print("result_cache_a (should have only 1 row):")
        result_cache_a.orderBy("id").show(truncate=False)

        assert (
            result_cache_a.count() == 1
        ), f"Expected 1 row in cache_a, but got {result_cache_a.count()}"
        assert_pyspark_df_equal(result_cache_a.orderBy("id"), expectedA.orderBy("id"))

        # Verify CDC metadata table has 2 rows (manually inserted + run-generated)
        cdc_metadata_table = spark.table(cache_b.full_cdc_table_name)
        cdc_row_count = cdc_metadata_table.count()

        print("CDC metadata table:")
        cdc_metadata_table.show(truncate=False)

        assert (
            cdc_row_count == 2
        ), f"Expected 2 rows in CDC metadata table, but got {cdc_row_count}"
        print(
            f"✅ Verified: CDC metadata table has {cdc_row_count} rows (manually inserted + run-generated)"
        )

        print("\n" + "=" * 80)
        print("✅ TEST COMPLETE: CDC timestamp filtering validated successfully!")
        print("=" * 80 + "\n")

    def test_cdc11(self, spark, max_workers):
        """
        Test CDC cache with C (regular cache) -> B (no cache) -> A (merged cache).

        Graph structure: C (cached) -> B -> A (CDC cached MERGE)

        This test demonstrates:
        1. First run with 3 records - caches data at C and A
        2. Manually add 1 more row to C's cache table
        3. Second run - should process only the new row from C through B to A
        4. Verify A's output has only 1 row (the new one)
        """

        # Create CDC cache for node C (regular cache, not merged)
        cache_c = CDCManagerCache(
            schema=self.test_schema,
            table="node_c",
            cdc_table="cdc_test11",
            merge_keys=["id"],
        )

        # Create CDC cache for node A (merged)
        cache_a = CDCManagerCache(
            schema=self.test_schema,
            table="node_a",
            cdc_table="cdc_test11",
            merge_keys=["id"],
        )

        @node(type="pyspark", cache=cache_c)
        def C(number_records: int = 3, start_id: int = 1):
            """Source node that generates data"""
            data = [(i, i * 10) for i in range(start_id, start_id + number_records)]
            return spark.createDataFrame(data, ["id", "value"])

        @node(type="pyspark", dependencies=[C])
        def B(C):
            """Non-cached transformation node"""
            return C.withColumn("score", F.col("value") * 2)

        @node(type="pyspark", dependencies=[B], cache=cache_a)
        def A(B):
            """Cached merged node"""
            return B.withColumn("doubled_score", F.col("score") * 2)

        # ===== First run with 3 records =====
        print("\n" + "=" * 80)
        print("🚀 RUN 1: Load with 3 records through C -> B -> A")
        print("=" * 80 + "\n")

        resultA = A.run(
            spark,
            parameters={C: {"number_records": 3}},
            max_workers=max_workers,
            debug=True,
        )

        # Assert run results for A
        expectedA_run1 = spark.createDataFrame(
            [(1, 10, 20, 40), (2, 20, 40, 80), (3, 30, 60, 120)],
            ["id", "value", "score", "doubled_score"],
        )

        print("resultA (run 1):")
        resultA.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedA (run 1):")
        expectedA_run1.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            resultA.drop("cdc_datetime_updated").orderBy("id"),
            expectedA_run1.orderBy("id"),
        )

        # Verify caches exist
        assert cache_c.exists(spark)
        assert cache_a.exists(spark)

        # ===== Manually add 1 more row to C =====
        print("\n" + "=" * 80)
        print("🔧 SETUP: Manually adding 1 more row to C's cache table")
        print("=" * 80 + "\n")

        from datetime import datetime

        new_row = spark.createDataFrame(
            [(4, 40, datetime.now())], ["id", "value", "cdc_datetime_updated"]
        )

        # Append to C's cache table
        new_row.write.format("delta").mode("append").save(cache_c.table_path)

        print("✅ Added row: id=4, value=40 to C's cache")
        print("\nC's cache table contents:")
        cache_c.read(spark).orderBy("id").show(truncate=False)

        # ===== Second run - should process only the new row =====
        print("\n" + "=" * 80)
        print("🚀 RUN 2: Re-run A (should process only new row from C)")
        print("=" * 80 + "\n")

        resultA_run2 = A.run(
            spark,
            cache={A: CacheMode.MERGE},
            max_workers=max_workers,
            debug=True,
        )

        # Assert run 2 results for A - should only contain the new row (id=4)
        expectedA_run2 = spark.createDataFrame(
            [(4, 40, 80, 160)],
            ["id", "value", "score", "doubled_score"],
        )

        print("resultA (run 2 - should only have 1 row for id=4):")
        resultA_run2.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedA (run 2 - only new row):")
        expectedA_run2.orderBy("id").show(truncate=False)

        # Assert only 1 row is returned
        assert (
            resultA_run2.count() == 1
        ), f"Expected 1 row in output, but got {resultA_run2.count()}"

        # Assert the row has the correct values
        assert_pyspark_df_equal(
            resultA_run2.drop("cdc_datetime_updated").orderBy("id"),
            expectedA_run2.orderBy("id"),
        )

        # Verify cache_a now has all 4 rows (merged)
        result_cache_a = cache_a.read(spark)
        expected_cache_a_all = spark.createDataFrame(
            [
                (1, 10, 20, 40),
                (2, 20, 40, 80),
                (3, 30, 60, 120),
                (4, 40, 80, 160),
            ],
            ["id", "value", "score", "doubled_score"],
        )

        print("result_cache_a (should have all 4 rows after merge):")
        result_cache_a.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expected_cache_a (all 4 rows):")
        expected_cache_a_all.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            result_cache_a.drop("cdc_datetime_updated").orderBy("id"),
            expected_cache_a_all.orderBy("id"),
        )

        print("\n" + "=" * 80)
        print("✅ TEST COMPLETE: CDC incremental processing validated successfully!")
        print("=" * 80 + "\n")

    def test_cdc12(self, spark, mocker, max_workers):
        """
        Test that CDC methods (read_cdc and write_cdc) are NOT called when cache is disabled.

        Graph structure: B (cached) -> A (cached)

        This test verifies that when providing input for B and disabling cache for A,
        the CDC read_cdc and write_cdc methods for both A and B should not be invoked.
        Since B is provided as input, it doesn't execute, and A has cache disabled.
        """

        # Create CDC cache for node B
        cache_b = CDCManagerCache(
            schema=self.test_schema,
            table="node_b",
            cdc_table="cdc_test_disable",
            merge_keys=["id"],
        )

        # Create CDC cache for node A
        cache_a = CDCManagerCache(
            schema=self.test_schema,
            table="node_a",
            cdc_table="cdc_test_disable",
            merge_keys=["id"],
        )

        @node(type="pyspark", cache=cache_b)
        def B():
            """Cached source node"""
            data = [(1, 10), (2, 20), (3, 30)]
            return spark.createDataFrame(data, ["id", "value"])

        @node(type="pyspark", dependencies=[B], cache=cache_a)
        def A(B):
            """Cached transformation node"""
            return B.withColumn("doubled", F.col("value") * 2)

        # Spy on the CDC methods for cache A
        read_spy_a = mocker.spy(cache_a, "read")

        # Spy on the CDC methods for cache B
        read_spy_b = mocker.spy(cache_b, "read")

        # Create input data for B
        input_b = spark.createDataFrame([(1, 10), (2, 20)], ["id", "value"])

        print("\n" + "=" * 80)
        print("🚀 Running A with input for B and cache DISABLED for A")
        print("=" * 80 + "\n")

        # Run A with input for B and cache disabled for A
        resultA = A.run(
            spark,
            inputs={B: input_b},
            cache={A: CacheMode.DISABLE},
            max_workers=max_workers,
            debug=True,
        )

        # Verify the result is correct
        expected = spark.createDataFrame(
            [(1, 10, 20), (2, 20, 40)],
            ["id", "value", "doubled"],
        )

        print("resultA:")
        resultA.orderBy("id").show(truncate=False)

        print("expected:")
        expected.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            resultA.orderBy("id"),
            expected.orderBy("id"),
        )

        # Assert that CDC methods for both A and B were NOT called
        read_spy_a.assert_not_called()
        read_spy_b.assert_not_called()

        # Verify that A's cache was NOT written (cache disabled)
        assert not cache_a.exists(spark), "Cache A should not exist when disabled"

        print("\n" + "=" * 80)
        print("✅ TEST COMPLETE: CDC methods for both A and B were not called!")
        print("=" * 80 + "\n")

    def test_cdc13(self, spark, max_workers):
        """
        Test CDC cache MERGE mode with pre-existing manually added cache data.

        Graph structure: B -> A (CDC cached MERGE)

        This test demonstrates:
        1. Manually add 1 row (id=2) to A's cache table before any run
        2. Run B -> A with MERGE mode, where B produces 1 row (id=1)
        3. Verify A's run output has only 1 row (id=1, the processed row from B)
        4. Verify A's cache has 2 rows total (id=1 and id=2)
        """

        # Create CDC cache for node A using Delta table in test schema
        cache_a = CDCManagerCache(
            schema=self.test_schema,
            table="node_a",
            cdc_table="cdc_test_manual_merge",
            merge_keys=["id"],
        )

        @node(type="pyspark")
        def B():
            """Source node that generates 1 row"""
            data = [(1, 10)]
            return spark.createDataFrame(data, ["id", "value"])

        @node(type="pyspark", dependencies=[B], cache=cache_a)
        def A(B):
            """Cached merged node"""
            return B.withColumn("score", F.col("value") * 2)

        # ===== Manually add 1 row to A's cache BEFORE any run =====
        print("\n" + "=" * 80)
        print("🔧 SETUP: Manually adding 1 row to A's cache table (before any run)")
        print("=" * 80 + "\n")

        from datetime import datetime

        manual_row = spark.createDataFrame(
            [(2, 20, 40, datetime.now())],
            ["id", "value", "score", "cdc_datetime_updated"],
        )

        # Create the cache table by writing the manual row
        manual_row.write.format("delta").mode("append").save(cache_a.table_path)

        # Create table pointing to the Delta location
        spark.sql(
            f"CREATE TABLE {cache_a.full_table_name} USING DELTA LOCATION '{cache_a.table_path}'"
        )

        print("✅ Added row: id=2, value=20, score=40 to A's cache")
        print("\nA's cache table contents (before run):")
        cache_a.read(spark).orderBy("id").show(truncate=False)

        # Verify cache exists for A
        assert cache_a.exists(spark)

        # Read from cache_a and verify it has the manually added row
        result_cache_a_before = cache_a.read(spark)
        expected_cache_a_before = spark.createDataFrame(
            [(2, 20, 40)],
            ["id", "value", "score"],
        )

        print(
            "\nA's cache table contents (before run - should have 1 manually added row):"
        )
        result_cache_a_before.drop("cdc_datetime_updated").orderBy("id").show(
            truncate=False
        )

        print("expected_cache_a_before (1 manual row):")
        expected_cache_a_before.orderBy("id").show(truncate=False)

        assert (
            result_cache_a_before.count() == 1
        ), f"Expected 1 row in cache_a before run, but got {result_cache_a_before.count()}"

        assert_pyspark_df_equal(
            result_cache_a_before.drop("cdc_datetime_updated").orderBy("id"),
            expected_cache_a_before.orderBy("id"),
        )

        # ===== Run with MERGE mode - B produces 1 row =====
        print("\n" + "=" * 80)
        print("🚀 RUN: Execute A with MERGE mode (B produces 1 row)")
        print("=" * 80 + "\n")

        resultA_run = A.run(
            spark,
            cache={A: CacheMode.MERGE},
            max_workers=max_workers,
            debug=True,
        )

        # Assert run results for A - should only have 1 row (processed from B)
        expectedA_output = spark.createDataFrame(
            [(1, 10, 20)],
            ["id", "value", "score"],
        )

        print("resultA (should only have 1 row from B):")
        resultA_run.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expectedA (output - only 1 row):")
        expectedA_output.orderBy("id").show(truncate=False)

        # Assert only 1 row is returned in output
        assert (
            resultA_run.count() == 1
        ), f"Expected 1 row in output, but got {resultA_run.count()}"

        assert_pyspark_df_equal(
            resultA_run.drop("cdc_datetime_updated").orderBy("id"),
            expectedA_output.orderBy("id"),
        )

        # Verify cache_a now has 2 rows (manually added + processed from B)
        result_cache_a = cache_a.read(spark)
        expected_cache_a_all = spark.createDataFrame(
            [
                (1, 10, 20),
                (2, 20, 40),
            ],
            ["id", "value", "score"],
        )

        print("\nA's cache table contents (after run - should have 2 rows):")
        result_cache_a.drop("cdc_datetime_updated").orderBy("id").show(truncate=False)

        print("expected_cache_a (all 2 rows):")
        expected_cache_a_all.orderBy("id").show(truncate=False)

        # Assert 2 rows in cache
        assert (
            result_cache_a.count() == 2
        ), f"Expected 2 rows in cache_a, but got {result_cache_a.count()}"

        assert_pyspark_df_equal(
            result_cache_a.drop("cdc_datetime_updated").orderBy("id"),
            expected_cache_a_all.orderBy("id"),
        )

        print("\n" + "=" * 80)
        print("✅ TEST COMPLETE: Manual cache merge validated successfully!")
        print("=" * 80 + "\n")

    def test_cdc14_static_node_no_filtering(self, spark, mocker, max_workers):
        """
        Test that static nodes skip CDC filtering when reading from cache.

        Graph structure: B (cached MERGE, marked as static) -> A

        This test demonstrates:
        1. First run with B having CacheMode.MERGE (produces 1 row with id=1)
        2. Manually add more data to B's cache (add row with id=2)
        3. Second run where A depends on B.static()
        4. Verify A's output has 2 rows (all data from B, no CDC filtering)
        5. Verify that _read_cdc_filter was never called (static skips CDC)
        """

        # Create CDC cache for node B using Delta table in test schema
        cache_b = CDCManagerCache(
            schema=self.test_schema,
            table="node_b",
            cdc_table="cdc_test_static",
            merge_keys=["id"],
        )

        @node(type="pyspark", cache=cache_b)
        def B():
            """Cached source node that generates 1 row"""
            data = [(1, 10)]
            return spark.createDataFrame(data, ["id", "value"])

        @node(type="pyspark", dependencies=[B.static()])
        def A(B):
            """Node A depends on B as static (no CDC filtering)"""
            return B.withColumn("score", F.col("value") * 2)

        # Spy on _read_cdc_filter to verify it's never called
        spy_read_cdc_filter = mocker.spy(cache_b, "_read_cdc_filter")

        # ===== First Run: B produces 1 row with MERGE mode =====
        print("\n" + "=" * 80)
        print("🚀 RUN 1: Execute A with B in MERGE mode (B produces 1 row)")
        print("=" * 80 + "\n")

        result1 = A.run(
            spark,
            cache={B: CacheMode.MERGE},
            max_workers=max_workers,
            debug=True,
        )

        print("Result from Run 1 (should have 1 row):")
        result1.orderBy("id").show(truncate=False)

        # Assert first run has 1 row
        assert result1.count() == 1, f"Expected 1 row in Run 1, got {result1.count()}"

        # Verify _read_cdc_filter was NOT called (static node)
        assert (
            spy_read_cdc_filter.call_count == 0
        ), f"Expected _read_cdc_filter NOT to be called in Run 1, but was called {spy_read_cdc_filter.call_count} times"

        print(
            "✅ Run 1 complete: 1 row processed, _read_cdc_filter NOT called (static)"
        )

        # ===== Manually add more data to B's cache =====
        print("\n" + "=" * 80)
        print("🔧 MANUAL UPDATE: Adding 1 more row to B's cache")
        print("=" * 80 + "\n")

        from datetime import datetime

        additional_row = spark.createDataFrame(
            [(2, 20, datetime.now())],
            ["id", "value", "cdc_datetime_updated"],
        )

        # Append to B's cache
        additional_row.write.format("delta").mode("append").save(cache_b.table_path)

        print("✅ Added row: id=2, value=20 to B's cache")
        print("\nB's cache table contents (after manual addition):")
        cache_b.read(spark).orderBy("id").show(truncate=False)

        # Verify B's cache now has 2 rows
        cache_b_contents = cache_b.read(spark)
        assert (
            cache_b_contents.count() == 2
        ), f"Expected 2 rows in B's cache, got {cache_b_contents.count()}"

        # Reset spy counter
        spy_read_cdc_filter.reset_mock()

        # ===== Second Run: A should get ALL data from B (no CDC filtering) =====
        print("\n" + "=" * 80)
        print("🚀 RUN 2: Execute A again (should get ALL 2 rows from static B)")
        print("=" * 80 + "\n")

        result2 = A.run(
            spark,
            max_workers=max_workers,
            debug=True,
        )

        print("Result from Run 2 (should have 2 rows - no CDC filtering):")
        result2.orderBy("id").show(truncate=False)

        # Assert second run has 2 rows (all data from B, because it's static)
        assert result2.count() == 2, f"Expected 2 rows in Run 2, got {result2.count()}"

        # Verify _read_cdc_filter was NOT called (static node)
        assert (
            spy_read_cdc_filter.call_count == 0
        ), f"Expected _read_cdc_filter NOT to be called in Run 2, but was called {spy_read_cdc_filter.call_count} times"

        # Verify the actual data
        expected_result = spark.createDataFrame(
            [(1, 10, 20), (2, 20, 40)],
            ["id", "value", "score"],
        )

        print("\nExpected result (2 rows):")
        expected_result.orderBy("id").show(truncate=False)

        assert_pyspark_df_equal(
            result2.drop("cdc_datetime_updated").orderBy("id"),
            expected_result.orderBy("id"),
        )

        print("\n" + "=" * 80)
        print("✅ TEST COMPLETE: Static node skipped CDC filtering in both runs!")
        print("=" * 80 + "\n")


class TestRunnerExecutionPlan:
    """Tests for Runner.create_execution_plan() method"""

    def test_execution_plan_linear_graph(self):
        """
        Test execution plan for simple linear graph: C -> B -> A

        Expected plan:
        - Level 0: [C]
        - Level 1: [B]
        - Level 2: [A]
        """

        @node(type="pandas")
        def C():
            """Source node"""
            import pandas as pd

            return pd.DataFrame({"id": [1, 2, 3]})

        @node(type="pandas", dependencies=[C])
        def B(C):
            """Middle node"""
            return C

        @node(type="pandas", dependencies=[B])
        def A(B):
            """Final node"""
            return B

        # Create graph
        from flypipe.run_context import RunContext

        run_context = RunContext()
        A.create_graph(run_context)
        execution_graph = A.node_graph.get_execution_graph(run_context)

        # Create runner and execution plan
        from flypipe.runner import Runner

        runner = Runner(node_graph=execution_graph, run_context=run_context)
        plan = runner.create_execution_plan(A)

        # Assert plan has 3 levels
        assert len(plan) == 3, f"Expected 3 levels, got {len(plan)}"

        # Assert level 0 has C
        level_0_names = [
            execution_graph.graph.nodes[key]["transformation"].__name__
            for key in plan[0]
        ]
        assert level_0_names == ["C"], f"Expected ['C'] in level 0, got {level_0_names}"

        # Assert level 1 has B
        level_1_names = [
            execution_graph.graph.nodes[key]["transformation"].__name__
            for key in plan[1]
        ]
        assert level_1_names == ["B"], f"Expected ['B'] in level 1, got {level_1_names}"

        # Assert level 2 has A
        level_2_names = [
            execution_graph.graph.nodes[key]["transformation"].__name__
            for key in plan[2]
        ]
        assert level_2_names == ["A"], f"Expected ['A'] in level 2, got {level_2_names}"

        print("✅ Linear graph execution plan correct")

    def test_execution_plan_diamond_graph(self):
        """
        Test execution plan for diamond graph:
            D
           / \
          B   C
           \ /
            A

        Expected plan:
        - Level 0: [D]
        - Level 1: [B, C] (parallel)
        - Level 2: [A]
        """

        @node(type="pandas")
        def D():
            """Source node"""
            import pandas as pd

            return pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})

        @node(type="pandas", dependencies=[D])
        def B(D):
            """Branch 1"""
            return D

        @node(type="pandas", dependencies=[D])
        def C(D):
            """Branch 2"""
            return D

        @node(type="pandas", dependencies=[B, C])
        def A(B, C):
            """Final node"""
            return B.merge(C, on="id")

        # Create graph
        from flypipe.run_context import RunContext

        run_context = RunContext()
        A.create_graph(run_context)
        execution_graph = A.node_graph.get_execution_graph(run_context)

        # Create runner and execution plan
        from flypipe.runner import Runner

        runner = Runner(node_graph=execution_graph, run_context=run_context)
        plan = runner.create_execution_plan(A)

        # Assert plan has 3 levels
        assert len(plan) == 3, f"Expected 3 levels, got {len(plan)}"

        # Assert level 0 has D
        level_0_names = [
            execution_graph.graph.nodes[key]["transformation"].__name__
            for key in plan[0]
        ]
        assert level_0_names == ["D"], f"Expected ['D'] in level 0, got {level_0_names}"

        # Assert level 1 has B and C (can be in any order)
        level_1_names = sorted(
            [
                execution_graph.graph.nodes[key]["transformation"].__name__
                for key in plan[1]
            ]
        )
        assert level_1_names == [
            "B",
            "C",
        ], f"Expected ['B', 'C'] in level 1, got {level_1_names}"

        # Assert level 2 has A
        level_2_names = [
            execution_graph.graph.nodes[key]["transformation"].__name__
            for key in plan[2]
        ]
        assert level_2_names == ["A"], f"Expected ['A'] in level 2, got {level_2_names}"

        # Assert parallelism is 2 for level 1
        assert (
            len(plan[1]) == 2
        ), f"Expected 2 nodes in level 1 (parallel), got {len(plan[1])}"

        print("✅ Diamond graph execution plan correct with parallelism")

    def test_execution_plan_with_provided_input(self):
        """
        Test execution plan when input is provided - should skip upstream nodes

        Graph: C -> B -> A
        Provide input for B

        Expected plan:
        - Level 0: [B] (provided input)
        - Level 1: [A]
        """

        @node(type="pandas")
        def C():
            """Source node - should be skipped"""
            import pandas as pd

            return pd.DataFrame({"id": [1, 2, 3]})

        @node(type="pandas", dependencies=[C])
        def B(C):
            """Middle node - provided as input"""
            return C

        @node(type="pandas", dependencies=[B])
        def A(B):
            """Final node"""
            return B

        # Create graph with provided input for B
        from flypipe.run_context import RunContext
        import pandas as pd

        provided_df = pd.DataFrame({"id": [1, 2]})
        run_context = RunContext(provided_inputs={B: provided_df})
        A.create_graph(run_context)
        execution_graph = A.node_graph.get_execution_graph(run_context)

        # Create runner and execution plan
        from flypipe.runner import Runner

        runner = Runner(node_graph=execution_graph, run_context=run_context)
        plan = runner.create_execution_plan(A)

        # Assert plan has 2 levels (C is skipped)
        assert (
            len(plan) == 2
        ), f"Expected 2 levels (C should be skipped), got {len(plan)}"

        # Assert level 0 has B (provided input)
        level_0_names = [
            execution_graph.graph.nodes[key]["transformation"].__name__
            for key in plan[0]
        ]
        assert level_0_names == ["B"], f"Expected ['B'] in level 0, got {level_0_names}"

        # Assert level 1 has A
        level_1_names = [
            execution_graph.graph.nodes[key]["transformation"].__name__
            for key in plan[1]
        ]
        assert level_1_names == ["A"], f"Expected ['A'] in level 1, got {level_1_names}"

        # Assert C is not in any level
        all_node_names = []
        for level in plan:
            all_node_names.extend(
                [
                    execution_graph.graph.nodes[key]["transformation"].__name__
                    for key in level
                ]
            )
        assert (
            "C" not in all_node_names
        ), "C should be skipped when B has provided input"

        print("✅ Execution plan correctly skips upstream when input is provided")

    def test_execution_plan_with_cached_node(self):
        """
        Test execution plan when a node is cached - should load from cache

        Graph: C -> B (cached) -> A

        Expected plan:
        - Level 0: [B] (cached, no upstream needed)
        - Level 1: [A]
        """
        from flypipe.cache.cache import Cache

        class DummyCache(Cache):
            def __init__(self):
                super().__init__()
                self._data = None

            def read(self):
                import pandas as pd

                return pd.DataFrame({"id": [1, 2]})

            def write(self, df):
                self._data = df

            def exists(self):
                return True

        @node(type="pandas")
        def C():
            """Source node - should be skipped"""
            import pandas as pd

            return pd.DataFrame({"id": [1, 2, 3]})

        @node(type="pandas", dependencies=[C], cache=DummyCache())
        def B(C):
            """Cached node"""
            return C

        @node(type="pandas", dependencies=[B])
        def A(B):
            """Final node"""
            return B

        # Create graph
        from flypipe.run_context import RunContext

        run_context = RunContext()
        A.create_graph(run_context)
        execution_graph = A.node_graph.get_execution_graph(run_context)

        # Create runner and execution plan
        from flypipe.runner import Runner

        runner = Runner(node_graph=execution_graph, run_context=run_context)
        plan = runner.create_execution_plan(A)

        # Assert plan has 2 levels (C is skipped because B is cached)
        assert (
            len(plan) == 2
        ), f"Expected 2 levels (C should be skipped), got {len(plan)}"

        # Assert level 0 has B (cached)
        level_0_names = [
            execution_graph.graph.nodes[key]["transformation"].__name__
            for key in plan[0]
        ]
        assert level_0_names == ["B"], f"Expected ['B'] in level 0, got {level_0_names}"

        # Assert level 1 has A
        level_1_names = [
            execution_graph.graph.nodes[key]["transformation"].__name__
            for key in plan[1]
        ]
        assert level_1_names == ["A"], f"Expected ['A'] in level 1, got {level_1_names}"

        print("✅ Execution plan correctly handles cached nodes")

    def test_execution_plan_complex_graph(self):
        """
        Test execution plan for complex graph with multiple branches:

              E   D
              |\ /|
              | X |
              |/ \|
              C   B
               \ /
                A

        Expected plan:
        - Level 0: [D, E] (parallel)
        - Level 1: [B, C] (parallel)
        - Level 2: [A]
        """

        @node(type="pandas")
        def D():
            """Source node 1"""
            import pandas as pd

            return pd.DataFrame({"id": [1, 2], "d_val": [10, 20]})

        @node(type="pandas")
        def E():
            """Source node 2"""
            import pandas as pd

            return pd.DataFrame({"id": [1, 2], "e_val": [100, 200]})

        @node(type="pandas", dependencies=[D, E])
        def B(D, E):
            """Branch 1 - depends on both D and E"""
            return D.merge(E, on="id")

        @node(type="pandas", dependencies=[D, E])
        def C(D, E):
            """Branch 2 - depends on both D and E"""
            return D.merge(E, on="id")

        @node(type="pandas", dependencies=[B, C])
        def A(B, C):
            """Final node"""
            return B

        # Create graph
        from flypipe.run_context import RunContext

        run_context = RunContext()
        A.create_graph(run_context)
        execution_graph = A.node_graph.get_execution_graph(run_context)

        # Create runner and execution plan
        from flypipe.runner import Runner

        runner = Runner(node_graph=execution_graph, run_context=run_context)
        plan = runner.create_execution_plan(A)

        # Assert plan has 3 levels
        assert len(plan) == 3, f"Expected 3 levels, got {len(plan)}"

        # Assert level 0 has D and E (can be in any order)
        level_0_names = sorted(
            [
                execution_graph.graph.nodes[key]["transformation"].__name__
                for key in plan[0]
            ]
        )
        assert level_0_names == [
            "D",
            "E",
        ], f"Expected ['D', 'E'] in level 0, got {level_0_names}"
        assert len(plan[0]) == 2, "Level 0 should have 2 nodes for parallel execution"

        # Assert level 1 has B and C (can be in any order)
        level_1_names = sorted(
            [
                execution_graph.graph.nodes[key]["transformation"].__name__
                for key in plan[1]
            ]
        )
        assert level_1_names == [
            "B",
            "C",
        ], f"Expected ['B', 'C'] in level 1, got {level_1_names}"
        assert len(plan[1]) == 2, "Level 1 should have 2 nodes for parallel execution"

        # Assert level 2 has A
        level_2_names = [
            execution_graph.graph.nodes[key]["transformation"].__name__
            for key in plan[2]
        ]
        assert level_2_names == ["A"], f"Expected ['A'] in level 2, got {level_2_names}"

        print("✅ Complex graph execution plan correct with maximum parallelism")

    def test_execution_plan_independent_branches(self):
        """
        Test execution plan for graph with independent branches:

          C     B
          |     |
          |     |
          A     (independent)

        When running A, B should not be included in the plan

        Expected plan:
        - Level 0: [C]
        - Level 1: [A]
        """

        @node(type="pandas")
        def C():
            """Source for A"""
            import pandas as pd

            return pd.DataFrame({"id": [1, 2]})

        @node(type="pandas")
        def B():
            """Independent source node"""
            import pandas as pd

            return pd.DataFrame({"id": [3, 4]})

        @node(type="pandas", dependencies=[C])
        def A(C):
            """Target node"""
            return C

        # Create graph for A
        from flypipe.run_context import RunContext

        run_context = RunContext()
        A.create_graph(run_context)
        execution_graph = A.node_graph.get_execution_graph(run_context)

        # Create runner and execution plan
        from flypipe.runner import Runner

        runner = Runner(node_graph=execution_graph, run_context=run_context)
        plan = runner.create_execution_plan(A)

        # Assert plan has 2 levels
        assert len(plan) == 2, f"Expected 2 levels, got {len(plan)}"

        # Assert level 0 has C
        level_0_names = [
            execution_graph.graph.nodes[key]["transformation"].__name__
            for key in plan[0]
        ]
        assert level_0_names == ["C"], f"Expected ['C'] in level 0, got {level_0_names}"

        # Assert level 1 has A
        level_1_names = [
            execution_graph.graph.nodes[key]["transformation"].__name__
            for key in plan[1]
        ]
        assert level_1_names == ["A"], f"Expected ['A'] in level 1, got {level_1_names}"

        # Assert B is not in any level (it's independent)
        all_node_names = []
        for level in plan:
            all_node_names.extend(
                [
                    execution_graph.graph.nodes[key]["transformation"].__name__
                    for key in level
                ]
            )
        assert "B" not in all_node_names, "B should not be in execution plan for A"

        print("✅ Execution plan correctly excludes independent branches")
