"""
Utility functions for tests
"""

import shutil
import os


def drop_table(spark, table: str):
    """
    Drop a Delta table and remove its directory.

    This function:
    1. Gets the table location from Spark metadata
    2. Drops the table
    3. Removes the table directory

    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    table : str
        Full table name (e.g., "schema.table")
    """
    print(f"Dropping '{table}'...")

    # Get table location from Spark metadata
    table_path = None
    if spark.catalog.tableExists(table):
        try:
            # Get table description
            desc_extended = spark.sql(f"DESCRIBE EXTENDED {table}").collect()
            # Find the Location row
            for row in desc_extended:
                if row.col_name == "Location":
                    table_path = row.data_type
                    # Remove 'file:' prefix if present
                    if table_path and table_path.startswith("file:"):
                        table_path = table_path[5:]
                    break
        except Exception as e:
            print(f"   Could not get table location: {e}")

    # Drop the table
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    # Remove the directory if it exists
    if table_path and os.path.exists(table_path):
        shutil.rmtree(table_path)
        print(f"   Removed directory: {table_path}")

    print(f"Table '{table}' dropped!")
