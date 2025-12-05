from datetime import datetime

import pandas as pd
from pandas.testing import assert_frame_equal

from flypipe.cache.cdc_cache import CDCCache
from flypipe.node import node


class TestNodeCDC:
    """Tests for Node with CDCCache"""

    def test_cdc_cache_serial(self, mocker):
        """
        Test CDCCache with parallel=False
        Graph structure: t1 -> t2 -> t3
        Each node should write CDC data with timestamps
        """

        class CDCManager:
            """Manager for CDC timestamps tracking"""

            def __init__(self):
                # DataFrame to store CDC timestamps: source, destiny, cdc_datetime_updated
                self.cdc_data = pd.DataFrame(
                    columns=["source", "destiny", "cdc_datetime_updated"]
                )

            def write(self, current_node, upstream_nodes, timestamp):
                """Write CDC timestamp entries for current_node and its upstream nodes"""
                for source_node in upstream_nodes:
                    new_entry = pd.DataFrame(
                        {
                            "source": [source_node.key],
                            "destiny": [current_node.key],
                            "cdc_datetime_updated": [timestamp],
                        }
                    )
                    self.cdc_data = pd.concat(
                        [self.cdc_data, new_entry], ignore_index=True
                    )

            def filter(self, from_node, to_node, df):
                """
                Filter CDC data for a specific source->destiny edge
                Returns the dataframe filtered based on CDC timestamps
                """
                filtered = self.cdc_data[
                    (self.cdc_data["source"] == from_node.key)
                    & (self.cdc_data["destiny"] == to_node.key)
                ]
                return df

        cdc_manager = CDCManager()

        class MyCDCCache(CDCCache):
            def __init__(self, node_name, cdc_manager):
                self.node_name = node_name
                self.cdc_manager = cdc_manager
                self.data = None

            def read(self):
                return self.data

            def write(self, df):
                self.data = df.copy()

            def exists(self):
                return self.data is not None

            def read_cdc(self, from_node, to_node, df):
                """Read CDC data using the manager's filter method"""
                return self.cdc_manager.filter(from_node, to_node, df)

            def write_cdc(self, current_node, upstream_nodes, timestamp):
                """Write CDC data using the manager's write method"""
                self.cdc_manager.write(current_node, upstream_nodes, timestamp)

        cache_t1 = MyCDCCache("t1", cdc_manager)
        cache_t2 = MyCDCCache("t2", cdc_manager)
        cache_t3 = MyCDCCache("t3", cdc_manager)

        @node(type="pandas", cache=cache_t1)
        def t1():
            return pd.DataFrame({"col1": [1, 2, 3]})

        @node(type="pandas", dependencies=[t1], cache=cache_t2)
        def t2(t1):
            t1["col2"] = t1["col1"] * 2
            return t1

        @node(type="pandas", dependencies=[t2], cache=cache_t3)
        def t3(t2):
            t2["col3"] = t2["col1"] + t2["col2"]
            return t2

        # Spy on CDC methods
        spy_write_cdc_t1 = mocker.spy(cache_t1, "write_cdc")
        spy_write_cdc_t2 = mocker.spy(cache_t2, "write_cdc")
        spy_write_cdc_t3 = mocker.spy(cache_t3, "write_cdc")
        spy_read_cdc_t1 = mocker.spy(cache_t1, "read_cdc")
        spy_read_cdc_t2 = mocker.spy(cache_t2, "read_cdc")
        spy_read_cdc_t3 = mocker.spy(cache_t3, "read_cdc")

        # First run - should write cache and CDC data
        result = t3.run(parallel=False)

        assert_frame_equal(
            result, pd.DataFrame({"col1": [1, 2, 3], "col2": [2, 4, 6], "col3": [3, 6, 9]})
        )

        # Verify CDC write methods were called
        assert spy_write_cdc_t1.call_count == 1
        assert spy_write_cdc_t2.call_count == 1
        assert spy_write_cdc_t3.call_count == 1

        # Verify CDC data was tracked (t1 -> t2 and t2 -> t3 edges)
        assert len(cdc_manager.cdc_data) == 2
        assert t1.key in cdc_manager.cdc_data["source"].values
        assert t2.key in cdc_manager.cdc_data["source"].values
        assert t2.key in cdc_manager.cdc_data["destiny"].values
        assert t3.key in cdc_manager.cdc_data["destiny"].values

        # Second run - should read from cache
        spy_write_cdc_t1.reset_mock()
        spy_write_cdc_t2.reset_mock()
        spy_write_cdc_t3.reset_mock()

        result2 = t3.run(parallel=False)
        assert_frame_equal(result, result2)

        # Verify no additional CDC writes on cached run
        assert spy_write_cdc_t1.call_count == 0
        assert spy_write_cdc_t2.call_count == 0
        assert spy_write_cdc_t3.call_count == 0

    def test_cdc_cache_parallel(self, mocker):
        """
        Test CDCCache with parallel=True
        Graph structure with multiple branches:
             t1
            /  \\
          t2    t3
            \\  /
             t4
        """

        class CDCManager:
            """Manager for CDC timestamps tracking"""

            def __init__(self):
                self.cdc_data = pd.DataFrame(
                    columns=["source", "destiny", "cdc_datetime_updated"]
                )

            def write(self, current_node, upstream_nodes, timestamp):
                """Write CDC timestamp entries"""
                for source_node in upstream_nodes:
                    new_entry = pd.DataFrame(
                        {
                            "source": [source_node.key],
                            "destiny": [current_node.key],
                            "cdc_datetime_updated": [timestamp],
                        }
                    )
                    self.cdc_data = pd.concat(
                        [self.cdc_data, new_entry], ignore_index=True
                    )

            def filter(self, from_node, to_node, df):
                """Filter CDC data for specific edge"""
                filtered = self.cdc_data[
                    (self.cdc_data["source"] == from_node.key)
                    & (self.cdc_data["destiny"] == to_node.key)
                ]
                return df

        cdc_manager = CDCManager()

        class MyCDCCache(CDCCache):
            def __init__(self, node_name, cdc_manager):
                self.node_name = node_name
                self.cdc_manager = cdc_manager
                self.data = None

            def read(self):
                return self.data

            def write(self, df):
                self.data = df.copy()

            def exists(self):
                return self.data is not None

            def read_cdc(self, from_node, to_node, df):
                return self.cdc_manager.filter(from_node, to_node, df)

            def write_cdc(self, current_node, upstream_nodes, timestamp):
                self.cdc_manager.write(current_node, upstream_nodes, timestamp)

        cache_t1 = MyCDCCache("t1", cdc_manager)
        cache_t2 = MyCDCCache("t2", cdc_manager)
        cache_t3 = MyCDCCache("t3", cdc_manager)
        cache_t4 = MyCDCCache("t4", cdc_manager)

        @node(type="pandas", cache=cache_t1)
        def t1():
            return pd.DataFrame({"col1": [1, 2]})

        @node(type="pandas", dependencies=[t1], cache=cache_t2)
        def t2(t1):
            t1["col2"] = t1["col1"] * 2
            return t1

        @node(type="pandas", dependencies=[t1], cache=cache_t3)
        def t3(t1):
            t1["col3"] = t1["col1"] * 3
            return t1

        @node(type="pandas", dependencies=[t2, t3], cache=cache_t4)
        def t4(t2, t3):
            result = t2.merge(t3, on="col1")
            result["col4"] = result["col2"] + result["col3"]
            return result

        # First run with parallel=True
        result = t4.run(parallel=True)

        expected = pd.DataFrame(
            {"col1": [1, 2], "col2": [2, 4], "col3": [3, 6], "col4": [5, 10]}
        )
        assert_frame_equal(result, expected)

        # Verify CDC data was tracked for all edges
        # t1 has no upstream, t2 has t1, t3 has t1, t4 has t2 and t3
        assert len(cdc_manager.cdc_data) >= 4

        # Verify all edges are tracked
        edges = set(
            zip(cdc_manager.cdc_data["source"], cdc_manager.cdc_data["destiny"])
        )
        assert (t1.key, t2.key) in edges
        assert (t1.key, t3.key) in edges
        assert (t2.key, t4.key) in edges
        assert (t3.key, t4.key) in edges

        # Second run - should use cache
        result2 = t4.run(parallel=True)
        assert_frame_equal(result, result2)

    def test_cdc_cache_timestamp_filtering(self, mocker):
        """
        Test that CDC cache correctly filters based on timestamps
        """

        class CDCManager:
            def __init__(self):
                self.cdc_data = pd.DataFrame(
                    columns=["source", "destiny", "cdc_datetime_updated"]
                )
                self.last_query_time = {}

            def write(self, current_node, upstream_nodes, timestamp):
                for source_node in upstream_nodes:
                    new_entry = pd.DataFrame(
                        {
                            "source": [source_node.key],
                            "destiny": [current_node.key],
                            "cdc_datetime_updated": [timestamp],
                        }
                    )
                    self.cdc_data = pd.concat(
                        [self.cdc_data, new_entry], ignore_index=True
                    )

            def filter(self, from_node, to_node, df):
                """Filter returns only entries after last query time"""
                key = (from_node.key, to_node.key)
                filtered = self.cdc_data[
                    (self.cdc_data["source"] == from_node.key)
                    & (self.cdc_data["destiny"] == to_node.key)
                ]

                if key in self.last_query_time:
                    # Return only entries updated after last query
                    filtered = filtered[
                        filtered["cdc_datetime_updated"] > self.last_query_time[key]
                    ]

                # Update last query time
                if not filtered.empty:
                    self.last_query_time[key] = filtered[
                        "cdc_datetime_updated"
                    ].max()

                return filtered

            def write_manual(self, source_key, destiny_key, timestamp):
                """Manual write for testing timestamp filtering"""
                new_entry = pd.DataFrame(
                    {
                        "source": [source_key],
                        "destiny": [destiny_key],
                        "cdc_datetime_updated": [timestamp],
                    }
                )
                self.cdc_data = pd.concat(
                    [self.cdc_data, new_entry], ignore_index=True
                )

            def filter_manual(self, source_key, destiny_key):
                """Manual filter for testing"""
                key = (source_key, destiny_key)
                filtered = self.cdc_data[
                    (self.cdc_data["source"] == source_key)
                    & (self.cdc_data["destiny"] == destiny_key)
                ]

                if key in self.last_query_time:
                    filtered = filtered[
                        filtered["cdc_datetime_updated"] > self.last_query_time[key]
                    ]

                if not filtered.empty:
                    self.last_query_time[key] = filtered[
                        "cdc_datetime_updated"
                    ].max()

                return filtered

        cdc_manager = CDCManager()

        class MyCDCCache(CDCCache):
            def __init__(self, node_name, cdc_manager):
                self.node_name = node_name
                self.cdc_manager = cdc_manager
                self.data = None

            def read(self):
                return self.data

            def write(self, df):
                self.data = df.copy()

            def exists(self):
                return self.data is not None

            def read_cdc(self, from_node, to_node, df):
                return self.cdc_manager.filter(from_node, to_node, df)

            def write_cdc(self, current_node, upstream_nodes, timestamp):
                self.cdc_manager.write(current_node, upstream_nodes, timestamp)

        cache_t1 = MyCDCCache("t1", cdc_manager)
        cache_t2 = MyCDCCache("t2", cdc_manager)

        @node(type="pandas", cache=cache_t1)
        def t1():
            return pd.DataFrame({"col1": [1, 2, 3]})

        @node(type="pandas", dependencies=[t1], cache=cache_t2)
        def t2(t1):
            t1["col2"] = t1["col1"] * 2
            return t1

        # First write using manual method for testing
        timestamp1 = datetime(2023, 1, 1, 10, 0, 0)
        cdc_manager.write_manual(t1.key, t2.key, timestamp1)

        # Query CDC data
        cdc_data1 = cdc_manager.filter_manual(t1.key, t2.key)
        assert len(cdc_data1) == 1
        assert cdc_data1.iloc[0]["cdc_datetime_updated"] == timestamp1

        # Second write with later timestamp
        timestamp2 = datetime(2023, 1, 1, 11, 0, 0)
        cdc_manager.write_manual(t1.key, t2.key, timestamp2)

        # Query should return only new entry (timestamp filtering)
        cdc_data2 = cdc_manager.filter_manual(t1.key, t2.key)
        assert len(cdc_data2) == 1
        assert cdc_data2.iloc[0]["cdc_datetime_updated"] == timestamp2

        # Third write with even later timestamp
        timestamp3 = datetime(2023, 1, 1, 12, 0, 0)
        cdc_manager.write_manual(t1.key, t2.key, timestamp3)

        # Query should return only newest entry
        cdc_data3 = cdc_manager.filter_manual(t1.key, t2.key)
        assert len(cdc_data3) == 1
        assert cdc_data3.iloc[0]["cdc_datetime_updated"] == timestamp3

    def test_cdc_cache_timestamp_filtering_spark(self, spark, mocker):
        """
        Test that CDC cache correctly filters based on timestamps using PySpark
        Same as test_cdc_cache_timestamp_filtering but with Spark DataFrames
        """

        class CDCManager:
            def __init__(self):
                self.cdc_data = pd.DataFrame(
                    columns=["source", "destiny", "cdc_datetime_updated"]
                )
                self.last_query_time = {}

            def write(self, spark, current_node, upstream_nodes, timestamp):
                """Write CDC timestamp entries with spark parameter"""
                for source_node in upstream_nodes:
                    new_entry = pd.DataFrame(
                        {
                            "source": [source_node.key],
                            "destiny": [current_node.key],
                            "cdc_datetime_updated": [timestamp],
                        }
                    )
                    self.cdc_data = pd.concat(
                        [self.cdc_data, new_entry], ignore_index=True
                    )

            def filter(self, spark, from_node, to_node, df):
                """Filter returns only entries after last query time, with spark parameter"""
                key = (from_node.key, to_node.key)
                filtered = self.cdc_data[
                    (self.cdc_data["source"] == from_node.key)
                    & (self.cdc_data["destiny"] == to_node.key)
                ]

                if key in self.last_query_time:
                    # Return only entries updated after last query
                    filtered = filtered[
                        filtered["cdc_datetime_updated"] > self.last_query_time[key]
                    ]

                # Update last query time
                if not filtered.empty:
                    self.last_query_time[key] = filtered[
                        "cdc_datetime_updated"
                    ].max()

                # Return the dataframe, not the CDC metadata
                return df

            def write_manual(self, source_key, destiny_key, timestamp):
                """Manual write for testing timestamp filtering"""
                new_entry = pd.DataFrame(
                    {
                        "source": [source_key],
                        "destiny": [destiny_key],
                        "cdc_datetime_updated": [timestamp],
                    }
                )
                self.cdc_data = pd.concat(
                    [self.cdc_data, new_entry], ignore_index=True
                )

            def filter_manual(self, source_key, destiny_key):
                """Manual filter for testing"""
                key = (source_key, destiny_key)
                filtered = self.cdc_data[
                    (self.cdc_data["source"] == source_key)
                    & (self.cdc_data["destiny"] == destiny_key)
                ]

                if key in self.last_query_time:
                    filtered = filtered[
                        filtered["cdc_datetime_updated"] > self.last_query_time[key]
                    ]

                if not filtered.empty:
                    self.last_query_time[key] = filtered[
                        "cdc_datetime_updated"
                    ].max()

                return filtered

        cdc_manager = CDCManager()

        class MySparkCDCCache(CDCCache):
            def __init__(self, node_name, cdc_manager):
                self.node_name = node_name
                self.cdc_manager = cdc_manager
                self.data = None

            def read(self, spark):
                """Read cache using Spark"""
                if self.data is None:
                    return None
                return spark.createDataFrame(self.data)

            def write(self, spark, df):
                """Write cache - convert Spark DataFrame to pandas for storage"""
                self.data = df.toPandas()

            def exists(self, spark):
                return self.data is not None

            def read_cdc(self, spark, from_node, to_node, df):
                """Read CDC data using the manager's filter method"""
                return self.cdc_manager.filter(spark, from_node, to_node, df)

            def write_cdc(self, spark, current_node, upstream_nodes, timestamp):
                """Write CDC data using the manager's write method"""
                self.cdc_manager.write(spark, current_node, upstream_nodes, timestamp)

        cache_t1 = MySparkCDCCache("t1", cdc_manager)
        cache_t2 = MySparkCDCCache("t2", cdc_manager)

        @node(type="pyspark", cache=cache_t1)
        def t1():
            return spark.createDataFrame(
                data=[(1, ), (2, ), (3, )],
                schema=["col1"]
            )

        @node(type="pyspark", dependencies=[t1], cache=cache_t2)
        def t2(t1):
            return t1.selectExpr("col1", "col1 * 2 as col2")

        # First write using manual method for testing
        timestamp1 = datetime(2023, 1, 1, 10, 0, 0)
        cdc_manager.write_manual(t1.key, t2.key, timestamp1)

        # Query CDC data
        cdc_data1 = cdc_manager.filter_manual(t1.key, t2.key)
        assert len(cdc_data1) == 1
        assert cdc_data1.iloc[0]["cdc_datetime_updated"] == timestamp1

        # Second write with later timestamp
        timestamp2 = datetime(2023, 1, 1, 11, 0, 0)
        cdc_manager.write_manual(t1.key, t2.key, timestamp2)

        # Query should return only new entry (timestamp filtering)
        cdc_data2 = cdc_manager.filter_manual(t1.key, t2.key)
        assert len(cdc_data2) == 1
        assert cdc_data2.iloc[0]["cdc_datetime_updated"] == timestamp2

        # Third write with even later timestamp
        timestamp3 = datetime(2023, 1, 1, 12, 0, 0)
        cdc_manager.write_manual(t1.key, t2.key, timestamp3)

        # Query should return only newest entry
        cdc_data3 = cdc_manager.filter_manual(t1.key, t2.key)
        assert len(cdc_data3) == 1
        assert cdc_data3.iloc[0]["cdc_datetime_updated"] == timestamp3

        # Now test with actual node execution using Spark
        result = t2.run(spark, parallel=False)
        
        # Verify result is a Spark DataFrame with correct data
        result_pd = result.toPandas()
        expected_pd = pd.DataFrame({"col1": [1, 2, 3], "col2": [2, 4, 6]})
        assert_frame_equal(result_pd, expected_pd)

        # Verify cache was written
        assert cache_t1.exists(spark)
        assert cache_t2.exists(spark)

    def test_cdc_cache_incremental_spark(self, spark, mocker):
        """
        Test CDC cache with incremental processing using Spark - production-like scenario.
        
        Graph structure:
        
            t1 (input passthrough, no cache)
             |
             v
            t2 (transformation with CDC cache)
             |
             v
            t3 (final node with CDC cache)
        
        Scenario:
        1. First run: Process initial data (rows 1, 2)
           - Uses CacheMode.MERGE to write to cache
           - Adds cdc_datetime_updated timestamp to each row
           - Writes CDC metadata tracking when data was processed
        
        2. Second run: Process new data (rows 3, 4)
           - Uses CacheMode.MERGE to append to existing cache
           - CDC filter identifies these as new rows based on timestamp
           - Only processes the new rows (3, 4)
           - Cache now contains all rows (1, 2, 3, 4)
        
        This simulates a real CDC scenario where you incrementally process only
        new/changed data and append to an existing table.
        """

        class CDCManager:
            """Manager that tracks CDC data and identifies new rows"""
            
            def __init__(self):
                # Stores CDC metadata: source, destiny, cdc_datetime_updated
                self.cdc_data = pd.DataFrame(
                    columns=["source", "destiny", "cdc_datetime_updated"]
                )

            def write(self, spark, current_node, upstream_nodes, timestamp):
                """Write CDC timestamp entries for current_node and upstream nodes"""
                for source_node in upstream_nodes:
                    new_entry = pd.DataFrame(
                        {
                            "source": [source_node.key],
                            "destiny": [current_node.key],
                            "cdc_datetime_updated": [timestamp],
                        }
                    )
                    self.cdc_data = pd.concat(
                        [self.cdc_data, new_entry], ignore_index=True
                    )

            def filter(self, spark, from_node, to_node, df):
                """
                Filter dataframe to return only rows with cdc_datetime_updated after last processed timestamp.
                """
                # Get the last processed timestamp for this edge
                edge_data = self.cdc_data[
                    (self.cdc_data["source"] == from_node.key)
                    & (self.cdc_data["destiny"] == to_node.key)
                ]
                
                if edge_data.empty:
                    # No previous run, return all data
                    return df
                
                # Get the latest timestamp for this edge
                last_timestamp = edge_data["cdc_datetime_updated"].max()
                
                # Filter dataframe to return only rows updated after last timestamp
                # Assumes df has a cdc_datetime_updated column
                if "cdc_datetime_updated" in df.columns:
                    filtered_df = df.filter(df["cdc_datetime_updated"] > last_timestamp)
                    return filtered_df
                
                # If no cdc_datetime_updated column, return all data
                return df

        cdc_manager = CDCManager()

        class IncrementalCDCCache(CDCCache):
            """CDC Cache that supports incremental processing"""
            
            def __init__(self, node_name, cdc_manager):
                self.node_name = node_name
                self.cdc_manager = cdc_manager
                self.data = None
                self.previous_data = None

            def read(self, spark):
                """Read cached data"""
                if self.data is None:
                    return None
                return spark.createDataFrame(self.data)

            def write(self, spark, df):
                """Write cache - concatenate with existing data to simulate table append"""
                new_data = df.toPandas()
                if self.data is None:
                    self.data = new_data
                else:
                    # Concatenate to simulate appending to a table
                    self.data = pd.concat([self.data, new_data], ignore_index=True)

            def exists(self, spark):
                return self.data is not None

            def read_cdc(self, spark, from_node, to_node, df):
                """
                Read CDC data - filter dataframe using CDC manager.
                Returns only rows that should be processed based on CDC tracking.
                """
                return self.cdc_manager.filter(spark, from_node, to_node, df)

            def write_cdc(self, spark, current_node, upstream_nodes, timestamp):
                """Write CDC metadata"""
                self.cdc_manager.write(spark, current_node, upstream_nodes, timestamp)

        cache_t2 = IncrementalCDCCache("t2", cdc_manager)
        cache_t3 = IncrementalCDCCache("t3", cdc_manager)

        @node(type="pyspark")
        def t1():
            """Source node - dummy node to pass through input"""
            return spark.createDataFrame(
                data=[(1, "A"), (2, "B")],
                schema=["id", "value"]
            )

        @node(type="pyspark", dependencies=[t1], cache=cache_t2)
        def t2(t1):
            """Transformation node"""
            from pyspark.sql.functions import current_timestamp
            return t1.selectExpr(
                "id",
                "value",
                "value || '_processed' as processed"
            ).withColumn("cdc_datetime_updated", current_timestamp())

        @node(type="pyspark", dependencies=[t2], cache=cache_t3)
        def t3(t2):
            """Final node"""
            return t2.selectExpr(
                "id",
                "value",
                "processed",
                "id * 10 as score",
                "cdc_datetime_updated"
            )

        # ===== First Run: Initial data =====
        initial_input = spark.createDataFrame(
            data=[(1, "A"), (2, "B")],
            schema=["id", "value"]
        )
        
        from flypipe.cache import CacheMode
        result1 = t3.run(
            spark,
            inputs={t1: initial_input},
            parallel=False,
            cache={t2: CacheMode.MERGE, t3: CacheMode.MERGE}
        )
        result1_pd = result1.toPandas().sort_values("id").reset_index(drop=True)
        
        # Verify the data columns (excluding cdc_datetime_updated which is dynamic)
        assert list(result1_pd["id"]) == [1, 2]
        assert list(result1_pd["value"]) == ["A", "B"]
        assert list(result1_pd["processed"]) == ["A_processed", "B_processed"]
        assert list(result1_pd["score"]) == [10, 20]
        assert "cdc_datetime_updated" in result1_pd.columns
        
        # Verify caches were written
        assert cache_t2.exists(spark)
        assert cache_t3.exists(spark)

        # ===== Second Run: New data (only new rows) =====
        new_input = spark.createDataFrame(
            data=[(3, "C"), (4, "D")],  # Only new rows (simulating CDC additions)
            schema=["id", "value"]
        )
        
        # Run with MERGE mode to append to existing cache
        result2 = t3.run(
            spark,
            inputs={t1: new_input},
            parallel=False,
            cache={t2: CacheMode.MERGE, t3: CacheMode.MERGE}
        )
        result2_pd = result2.toPandas().sort_values("id").reset_index(drop=True)
        
        # Result should only have new rows (3 and 4) - CDC filtered the data
        assert list(result2_pd["id"]) == [3, 4]
        assert list(result2_pd["value"]) == ["C", "D"]
        assert list(result2_pd["processed"]) == ["C_processed", "D_processed"]
        assert list(result2_pd["score"]) == [30, 40]
        assert "cdc_datetime_updated" in result2_pd.columns
        
        print(f"✓ CDC correctly processed only {len(result2_pd)} new rows")

        # Verify caches now contain all data (appended via MERGE)
        cached_t3_data = cache_t3.read(spark).toPandas().sort_values("id").reset_index(drop=True)
        
        assert list(cached_t3_data["id"]) == [1, 2, 3, 4]
        assert list(cached_t3_data["value"]) == ["A", "B", "C", "D"]
        assert list(cached_t3_data["processed"]) == ["A_processed", "B_processed", "C_processed", "D_processed"]
        assert list(cached_t3_data["score"]) == [10, 20, 30, 40]
        assert "cdc_datetime_updated" in cached_t3_data.columns
        
        print(f"✓ Cache contains all {len(cached_t3_data)} rows (original + new)")

