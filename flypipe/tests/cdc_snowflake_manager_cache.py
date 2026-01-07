"""
CDC Manager Cache implementation for Snowflake Snowpark.

This module provides a Snowflake-based CDC cache implementation for local testing.
"""

import pandas as pd
from flypipe.cache import Cache
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import when_matched, when_not_matched

from flypipe.utils import get_logger

logger = get_logger()


class CDCSnowflakeManagerCache(Cache):
    """
    Snowflake Snowpark-based CDC cache for local testing scenarios.

    This cache stores:
    - Data in a Snowpark table (with cdc_datetime_updated column)
    - CDC metadata in a separate table tracking source->destination relationships

    All methods work with Snowpark DataFrames.

    For local testing, tables are created in-memory and managed by the Snowpark session.

    Parameters
    ----------
    schema : str
        Schema/database name for the data table
    table : str
        Table name for the data
    merge_keys : list of str
        Column names to use as keys for MERGE INTO operations
    cdc_table : str, optional
        Table name for CDC metadata (default: "cdc_metadata")
    """

    def __init__(
        self,
        schema="default",
        table="data",
        merge_keys=None,
        cdc_table="cdc_metadata",
    ):
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

        # Build full table name for data
        self.full_table_name = f"{schema}.{table}"

        # Build full table name for CDC metadata
        self.full_cdc_table_name = f"{schema}.{cdc_table}"

        self.name = self.full_table_name

    def read(self, session, from_node=None, to_node=None, is_static=False):
        """
        Read cached data from Snowpark table with optional CDC filtering.

        Parameters
        ----------
        session : SnowflakeSession
            Active Snowflake session
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
            f"              📖 CDCSnowflakeManagerCache.read() called for '{self.full_table_name}'"
        )
        result_df = session.table(self.full_table_name)

        # If CDC parameters are provided and node is not static, do CDC filtering
        if from_node is not None and to_node is not None and not is_static:
            result_df = self._read_cdc_filter(session, from_node, to_node, result_df)
        else:
            row_count = result_df.count()
            if is_static:
                logger.debug(
                    f"               📖 Loaded {row_count} rows from Snowpark table (static node, no CDC filtering)"
                )
            else:
                logger.debug(
                    f"               📖 Loaded {row_count} rows from Snowpark table"
                )

        return result_df

    def write(
        self,
        session,
        df,
        upstream_nodes=None,
        to_node=None,
        datetime_started_transformation=None,
    ):
        """
        Write data to Snowpark table or CDC metadata with unified interface.

        Parameters
        ----------
        session : SnowflakeSession
            Active Snowflake session
        df : DataFrame
            Snowpark DataFrame to cache
        upstream_nodes : List[Node], optional
            List of upstream cached nodes for CDC tracking
        to_node : Node, optional
            Destination node for CDC tracking
        datetime_started_transformation : datetime, optional
            Timestamp when transformation started for CDC tracking
        """
        logger.debug(
            f"         💾 CDCSnowflakeManagerCache.write() called for '{self.full_table_name}'"
        )

        # Ensure cdc_datetime_updated column exists
        if "CDC_DATETIME_UPDATED" not in df.columns:
            df = df.with_column("CDC_DATETIME_UPDATED", F.current_timestamp())
        else:
            df = df.with_column("CDC_DATETIME_UPDATED", F.col("CDC_DATETIME_UPDATED"))

        # Check if table exists
        table_exists = self.exists(session)

        if not table_exists:
            # Table doesn't exist - create it
            logger.debug(
                f"         💾 Table doesn't exist, creating: {self.full_table_name}"
            )
            df.write.mode("overwrite").save_as_table(self.full_table_name)

            row_count = df.count()
            logger.debug(f"         💾 Created table with {row_count} rows")
        else:
            # Table exists - use merge (upsert)
            logger.debug(
                f"         💾 Table exists, using MERGE with keys: {self.merge_keys}"
            )

            # Get existing table as a Snowpark Table object
            target_table = session.table(self.full_table_name)

            # Build join expression based on merge keys
            # Example: (target["ID"] == df["ID"]) & (target["KEY2"] == df["KEY2"])
            join_conditions = [target_table[key] == df[key] for key in self.merge_keys]
            join_expr = join_conditions[0]
            for condition in join_conditions[1:]:
                join_expr = join_expr & condition

            # Build update dictionary - update all columns except merge keys
            # Use source values for all columns
            update_dict = {col: df[col] for col in df.columns}

            # Build insert dictionary - insert all columns
            insert_dict = {col: df[col] for col in df.columns}

            # Perform the merge using Snowpark's native merge API
            merge_result = target_table.merge(
                df,
                join_expr,
                [
                    when_matched().update(update_dict),
                    when_not_matched().insert(insert_dict),
                ],
            )

            row_count = df.count()
            logger.debug(
                f"         💾 Merged {row_count} rows using keys: {self.merge_keys} "
                f"(inserted={merge_result.rows_inserted}, updated={merge_result.rows_updated})"
            )

        if upstream_nodes:
            for upstream_node in upstream_nodes:
                self._write_cdc_metadata(
                    session, upstream_node, to_node, datetime_started_transformation
                )
        else:
            logger.debug("         -> No upstream nodes, skipping CDC metadata write")

    def exists(self, session):
        """Check if Snowpark table exists"""
        try:
            # Try to access the table
            session.table(self.full_table_name).limit(1).collect()
            exists = True
        except Exception:
            # Table doesn't exist or can't be accessed
            exists = False

        logger.debug(
            f"         🔍 CDCSnowflakeManagerCache.exists() called for '{self.full_table_name}': {exists}"
        )
        return exists

    def _read_cdc_filter(self, session, from_node, to_node, df):
        """
        Filter data based on CDC timestamps from Snowpark table metadata (internal method).

        Returns only rows that haven't been processed for this specific
        source->destination combination.

        Parameters
        ----------
        session : SnowflakeSession
            Active Snowflake session
        from_node : Node
            The source node providing the data
        to_node : Node
            The destination/target node of the current execution
        df : DataFrame
            The Snowpark dataframe to filter

        Returns
        -------
        DataFrame
            Filtered Snowpark dataframe containing only new/changed rows
        """

        if "CDC_DATETIME_UPDATED" not in df.columns:
            logger.debug(
                f"              📊 No CDC datetime updated column found, returning all {df.count()} rows"
            )
            return df

        # Check if CDC metadata table exists
        cdc_table_exists = False
        try:
            session.table(self.full_cdc_table_name).limit(1).collect()
            cdc_table_exists = True
        except:  # noqa: E722
            pass

        if not cdc_table_exists:
            logger.debug(
                f"              📊 No CDC metadata table found, returning all {df.count()} rows"
            )
            return df

        cdc_df = session.table(self.full_cdc_table_name)

        # Find the last processed timestamp for this source + destination combination
        edge_data = cdc_df.filter(
            (F.col("SOURCE") == from_node.__name__)
            & (F.col("DESTINATION") == to_node.__name__)
        )

        if edge_data.count() == 0:
            logger.debug(
                f"              📊 No CDC history for this edge, returning all {df.count()} rows"
            )
            return df

        last_timestamp = edge_data.agg(
            F.max(F.col("CDC_DATETIME_UPDATED")).alias("MAX_TS")
        ).collect()[0]["MAX_TS"]
        logger.debug(f"              📊 Last processed: {last_timestamp}")

        # Convert pandas Timestamp to Snowflake literal if needed
        if hasattr(last_timestamp, "to_pydatetime"):
            last_timestamp = last_timestamp.to_pydatetime()

        filtered_df = df.filter(F.col("CDC_DATETIME_UPDATED") > F.lit(last_timestamp))
        row_count = filtered_df.count()
        total_count = df.count()
        logger.debug(
            f"              📊 Filtered to {row_count} new rows (out of {total_count} total)"
        )
        return filtered_df

    def create_cdc_table(self, session):
        """
        Ensure CDC metadata table exists.

        Parameters
        ----------
        session : SnowflakeSession
            Active Snowflake session
        """
        try:
            session.table(self.full_cdc_table_name).limit(1).collect()
            # Table exists, do nothing
        except:  # noqa: E722
            # Table doesn't exist, create it
            try:
                # Create empty dataframe with explicit schema
                from snowflake.snowpark.types import (
                    StructType,
                    StructField,
                    StringType,
                    TimestampType,
                )

                schema = StructType(
                    [
                        StructField("SOURCE", StringType()),
                        StructField("DESTINATION", StringType()),
                        StructField("CDC_DATETIME_UPDATED", TimestampType()),
                    ]
                )

                empty_cdc_df = session.create_dataframe(
                    [],
                    schema=schema,
                )
                empty_cdc_df.write.mode("overwrite").save_as_table(
                    self.full_cdc_table_name
                )
            except Exception as e:
                # Table might have been created by another thread, check again
                try:
                    session.table(self.full_cdc_table_name).limit(1).collect()
                except:  # noqa: E722
                    raise e

    def _write_cdc_metadata(self, session, upstream_node, to_node, timestamp):
        """
        Write CDC metadata to Snowpark table tracking when data was processed (internal method).

        Records the processing timestamp for a source->destination combination,
        allowing future runs to identify which data has already been processed.

        Parameters
        ----------
        session : SnowflakeSession
            Active Snowflake session
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
            "SOURCE": upstream_node.__name__,
            "DESTINATION": to_node.__name__,
            "CDC_DATETIME_UPDATED": timestamp,
        }

        # Create Snowpark DataFrame from CDC entry
        new_cdc_df = session.create_dataframe(
            pd.DataFrame([cdc_entry]),
            schema=["SOURCE", "DESTINATION", "CDC_DATETIME_UPDATED"],
        )

        # Check if CDC table exists
        self.create_cdc_table(session)

        # Append to CDC metadata table
        try:
            existing_cdc = session.table(self.full_cdc_table_name)
            combined = existing_cdc.union_all(new_cdc_df)
            combined.write.mode("overwrite").save_as_table(self.full_cdc_table_name)
        except:  # noqa: E722
            # Table might not exist yet, create it
            new_cdc_df.write.mode("overwrite").save_as_table(self.full_cdc_table_name)
