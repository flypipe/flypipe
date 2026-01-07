"""
CDC Manager Cache implementation for production-like scenarios.

This module provides a Spark Delta table-based CDC cache implementation.
"""

import pandas as pd
from flypipe.cache import Cache
import pyspark.sql.functions as F

from flypipe.utils import get_logger

logger = get_logger()


class CDCManagerCache(Cache):
    """
    Spark Delta table-based CDC cache for production-like scenarios.

    This cache stores:
    - Data in a Delta table (with cdc_datetime_updated column)
    - CDC metadata in a separate Delta table tracking source->destination relationships

    All methods work exclusively with Spark DataFrames (not pandas).

    Tables are created by:
    1. Writing Delta files to a path on shared storage
    2. Creating a table pointing to that Delta location

    Parameters
    ----------
    catalog : str, optional
        Catalog name for the data table (default: None, uses default catalog)
    schema : str
        Schema/database name for the data table
    table : str
        Table name for the data
    merge_keys : list of str
        Column names to use as keys for MERGE INTO operations
    cdc_table : str, optional
        Table name for CDC metadata (default: "cdc_metadata")
    cdc_catalog : str, optional
        Catalog name for CDC metadata table (default: None, uses same as data table)
    cdc_schema : str, optional
        Schema name for CDC metadata table (default: None, uses same as data table)
    base_path : str, optional
        Base path for storing Delta files (default: "/shared/delta/tables")
    """

    def __init__(
        self,
        catalog=None,
        schema="default",
        table="data",
        merge_keys=None,
        cdc_table="cdc_metadata",
        cdc_catalog=None,
        cdc_schema=None,
        base_path="/shared/delta/tables",
    ):
        self.catalog = catalog
        self.schema = schema
        self.table = table

        # Validate and set merge_keys
        if not isinstance(merge_keys, list):
            raise TypeError(
                f"merge_keys must be a list, got {type(merge_keys).__name__}"
            )
        elif len(merge_keys) == 0:
            raise ValueError("merge_keys cannot be an empty list")
        else:
            self.merge_keys = merge_keys

        self.cdc_table = cdc_table
        self.cdc_catalog = cdc_catalog if cdc_catalog is not None else catalog
        self.cdc_schema = cdc_schema if cdc_schema is not None else schema
        self.base_path = base_path

        # Build full table name for data
        if catalog:
            self.full_table_name = f"{catalog}.{schema}.{table}"
            self.table_path = f"{base_path}/{catalog}/{schema}/{table}"
        else:
            self.full_table_name = f"{schema}.{table}"
            self.table_path = f"{base_path}/{schema}/{table}"

        # Build full table name for CDC metadata
        if self.cdc_catalog:
            self.full_cdc_table_name = (
                f"{self.cdc_catalog}.{self.cdc_schema}.{cdc_table}"
            )
            self.cdc_table_path = (
                f"{base_path}/{self.cdc_catalog}/{self.cdc_schema}/{cdc_table}"
            )
        else:
            self.full_cdc_table_name = f"{self.cdc_schema}.{cdc_table}"
            self.cdc_table_path = f"{base_path}/{self.cdc_schema}/{cdc_table}"

        self.name = self.full_table_name

    def read(self, spark, from_node=None, to_node=None, is_static=False):
        """
        Read cached data from Delta table with optional CDC filtering.

        Parameters
        ----------
        spark : SparkSession
            Active Spark session
        from_node : Node, optional
            Source node for CDC filtering (skipped if is_static is True)
        to_node : Node, optional
            Destination node for CDC filtering
        is_static : bool, optional
            If True, skip CDC filtering and load complete cached data (default: False)

        Returns
        -------
        DataFrame
            Cached data or CDC-filtered data
        """
        # Just read the cached data
        logger.debug(
            f"              📖 CDCManagerCache.read() called for '{self.full_table_name}'"
        )
        result_df = spark.table(self.full_table_name)

        # If CDC parameters are provided and node is not static, do CDC filtering
        if from_node is not None and to_node is not None and not is_static:
            result_df = self._read_cdc_filter(spark, from_node, to_node, result_df)
        else:
            row_count = result_df.count()
            if is_static:
                logger.debug(
                    f"               📖 Loaded {row_count} rows from Delta table (static node, no CDC filtering)"
                )
            else:
                logger.debug(
                    f"               📖 Loaded {row_count} rows from Delta table"
                )

        return result_df

    def write(
        self,
        spark,
        df,
        upstream_nodes=None,
        to_node=None,
        datetime_started_transformation=None,
    ):
        """
        Write data to Delta table or CDC metadata with unified interface.

        Parameters
        ----------
        spark : SparkSession
            Active Spark session
        *args
            Additional positional arguments
        df : DataFrame
            DataFrame to cache (for regular cache write)
        upstream_nodes : List[Node], optional
            List of upstream cached nodes for CDC tracking
        to_node : Node, optional
            Destination node for CDC tracking
        datetime_started_transformation : datetime, optional
            Timestamp when transformation started for CDC tracking
        **kwargs
            Additional keyword arguments
        """
        # If CDC parameters are provided, write CDC metadata
        logger.debug(
            f"         💾 CDCManagerCache.write() called for '{self.full_table_name}'"
        )

        if "cdc_datetime_updated" not in df.columns:
            df = df.withColumn("cdc_datetime_updated", F.current_timestamp())
        else:
            df = df.withColumn("cdc_datetime_updated", F.col("cdc_datetime_updated"))

        # Check if table exists
        table_exists = spark.catalog.tableExists(self.full_table_name)

        if not table_exists:
            # Table doesn't exist - create it
            logger.debug(
                f"         💾 Table doesn't exist, creating: {self.full_table_name}"
            )
            df.write.format("delta").mode("append").save(self.table_path)

            # Create table pointing to the Delta location
            spark.sql(
                f"CREATE TABLE {self.full_table_name} USING DELTA LOCATION '{self.table_path}'"
            )

            row_count = df.count()
            logger.debug(f"         💾 Created table with {row_count} rows")
        else:
            # Table exists - use MERGE INTO
            logger.debug(
                f"         💾 Table exists, using MERGE INTO with keys: {self.merge_keys}"
            )

            # Create a temporary view for the new data
            temp_view_name = f"temp_{self.table.replace('.', '_')}_{id(df)}"
            df.createOrReplaceTempView(temp_view_name)

            # Build MERGE INTO statement
            merge_condition = " AND ".join(
                [f"target.{key} = source.{key}" for key in self.merge_keys]
            )

            merge_sql = f"""
            MERGE INTO {self.full_table_name} AS target
            USING {temp_view_name} AS source
            ON {merge_condition}
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
            """

            logger.debug("         💾 Executing MERGE INTO...")
            spark.sql(merge_sql)

            # Drop the temporary view
            spark.catalog.dropTempView(temp_view_name)

            row_count = df.count()
            logger.debug(
                f"         💾 Merged {row_count} rows using keys: {self.merge_keys}"
            )

        if upstream_nodes:
            for upstream_node in upstream_nodes:
                self._write_cdc_metadata(
                    spark, upstream_node, to_node, datetime_started_transformation
                )
        else:
            logger.debug("         -> No upstream nodes, skipping CDC metadata write")

    def exists(self, spark):
        """Check if Delta table exists"""
        exists = spark.catalog.tableExists(self.full_table_name)
        logger.debug(
            f"         🔍 CDCManagerCache.exists() called for '{self.full_table_name}': {exists}"
        )
        return exists

    def _read_cdc_filter(self, spark, from_node, to_node, df):
        """
        Filter data based on CDC timestamps from Delta table metadata (internal method).

        Returns only rows that haven't been processed for this specific
        source->destination combination.

        Parameters
        ----------
        spark : SparkSession
            Active Spark session
        from_node : Node
            The source node providing the data
        to_node : Node
            The destination/target node of the current execution
        df : DataFrame
            The Spark dataframe to filter

        Returns
        -------
        DataFrame
            Filtered Spark dataframe containing only new/changed rows
        """

        if "cdc_datetime_updated" not in df.columns:
            logger.debug(
                f"              📊 No CDC datetime updated column found, returning all {df.count()} rows"
            )
            return df

        # Check if CDC metadata table exists
        if not spark.catalog.tableExists(self.full_cdc_table_name):
            logger.debug(
                f"              📊 No CDC metadata table found, returning all {df.count()} rows"
            )
            return df

        cdc_df = spark.table(self.full_cdc_table_name)

        # Find the last processed timestamp for this source + destination combination
        edge_data = cdc_df.filter(
            (cdc_df.source == from_node.__name__)
            & (cdc_df.destination == to_node.__name__)
        )

        if edge_data.count() == 0:
            logger.debug(
                f"              📊 No CDC history for this edge, returning all {df.count()} rows"
            )
            return df

        last_timestamp = edge_data.agg(
            F.max("cdc_datetime_updated").alias("max_ts")
        ).collect()[0]["max_ts"]
        logger.debug(f"              📊 Last processed: {last_timestamp}")

        filtered_df = df.filter(df.cdc_datetime_updated > last_timestamp)
        row_count = filtered_df.count()
        total_count = df.count()
        logger.debug(
            f"              📊 Filtered to {row_count} new rows (out of {total_count} total)"
        )
        return filtered_df

    def create_cdc_table(self, spark):
        """
        Ensure CDC metadata table exists (thread-safe for parallel execution).

        This method should be called before parallel execution to avoid
        concurrent table creation conflicts.

        Parameters
        ----------
        spark : SparkSession
            Active Spark session
        """
        if not spark.catalog.tableExists(self.full_cdc_table_name):
            try:
                empty_cdc_df = spark.createDataFrame(
                    [],
                    schema="source STRING, destination STRING, cdc_datetime_updated TIMESTAMP",
                )
                empty_cdc_df.write.format("delta").mode("append").partitionBy(
                    "source", "destination"
                ).save(self.cdc_table_path)
                spark.sql(
                    f"CREATE TABLE {self.full_cdc_table_name} USING DELTA LOCATION '{self.cdc_table_path}'"
                )
            except Exception as e:
                # Table might have been created by another thread, check again
                if not spark.catalog.tableExists(self.full_cdc_table_name):
                    raise e

    def _write_cdc_metadata(self, spark, upstream_node, to_node, timestamp):
        """
        Write CDC metadata to Delta table tracking when data was processed (internal method).

        Records the processing timestamp for a source->destination combination,
        allowing future runs to identify which data has already been processed.

        Parameters
        ----------
        spark : SparkSession
            Active Spark session
        upstream_node : Node
            The node that provided data
        to_node : Node
            The destination/target node of the current execution
        timestamp : datetime
            The timestamp when processing occurred
        """
        logger.debug(
            f"          ✍️  Writing CDC: {upstream_node.__name__} -> {to_node.__name__}"
        )
        # Create CDC metadata entry
        cdc_entry = {
            "source": upstream_node.__name__,
            "destination": to_node.__name__,
            "cdc_datetime_updated": timestamp,
        }

        # Create Spark DataFrame from CDC entry
        new_cdc_df = spark.createDataFrame(pd.DataFrame([cdc_entry]))

        # Write DataFrame to Delta format at the CDC path (append mode)
        new_cdc_df.write.format("delta").mode("append").partitionBy(
            "source", "destination"
        ).save(self.cdc_table_path)
