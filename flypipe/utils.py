import json
import logging
from enum import Enum
from pandas.testing import assert_frame_equal

from flypipe.exceptions import (
    DataframeDifferentDataError,
    DataframeSchemasDoNotMatchError,
    DataframeTypeNotSupportedError,
)

import pandas as pd
import pyspark.sql.dataframe as sql
import sparkleframe.polarsdf.dataframe as sparkle_dataframe


def sparkleframe_is_active():
    from pyspark.sql import SparkSession

    spark_session_module = SparkSession.__module__.split(".")[0]
    return spark_session_module == "sparkleframe"


if sparkleframe_is_active():
    # if using sparkleframe activate, it will fail because they do not implement pyspark.pandas
    import pandas as ps

    # if using sparkleframe activate, it will fail because they do not implement pyspark.sql.connect
    import pyspark.sql.dataframe as sql_connect
else:
    import pyspark.pandas as ps
    import pyspark.sql.connect.dataframe as sql_connect


class DataFrameType(Enum):
    """
    Enum of possible dataframe types
    """

    PANDAS = "pandas"
    PANDAS_ON_SPARK = "pandas_on_spark"
    PYSPARK = "pyspark"


def assert_schemas_are_equals(df1, df2) -> None:
    if dataframe_type(df1) == DataFrameType.PANDAS:
        if not df1.dtypes.equals(df2.dtypes):
            raise DataframeSchemasDoNotMatchError(
                f"Schema of df1 {df1.dtypes} != schema df2 {df2.dtypes}"
            )

    elif dataframe_type(df1) in [DataFrameType.PYSPARK, DataFrameType.PANDAS_ON_SPARK]:
        schema_df1 = json.dumps(sorted(df1.dtypes, key=lambda t: t[0]))
        schema_df2 = json.dumps(sorted(df2.dtypes, key=lambda t: t[0]))
        if schema_df1 != schema_df2:
            raise DataframeSchemasDoNotMatchError(
                f"Schema of df1 {schema_df1} != schema df2 {schema_df2}"
            )


def assert_dataframes_equals(df1, df2) -> None:
    df1_type = dataframe_type(df1)
    assert df1_type == dataframe_type(df2), f"Error: df1 {type(df1)} != {type(df2)}"

    if df1_type == DataFrameType.PANDAS:
        assert_frame_equal(df1, df2)

    elif df1_type in [DataFrameType.PYSPARK, DataFrameType.PANDAS_ON_SPARK]:
        if df1_type == DataFrameType.PANDAS_ON_SPARK:
            df1 = df1.to_spark()
            df2 = df2.to_spark()

        assert_schemas_are_equals(df1, df2)

        if df1.exceptAll(df2).count() != df2.exceptAll(df1).count():
            raise DataframeDifferentDataError()


def dataframe_type(df) -> DataFrameType:
    if isinstance(df, pd.DataFrame):
        return DataFrameType.PANDAS
    if isinstance(df, ps.DataFrame):
        return DataFrameType.PANDAS_ON_SPARK
    if (
        isinstance(df, sql.DataFrame)
        or isinstance(df, sql_connect.DataFrame)
        or isinstance(df, sparkle_dataframe.DataFrame)
    ):
        return DataFrameType.PYSPARK
    raise DataframeTypeNotSupportedError(type(df))


# TODO: add tests to get_schema
def get_schema(df, columns=None):
    if columns is None:
        columns = {}

    if dataframe_type(df) == DataFrameType.PYSPARK:
        return {
            s.name: s.dataType
            for s in (df if not columns else df.select(columns)).schema
        }

    if not columns:
        return df.dtypes
    return df.dtypes[columns]


def log(logger, message):
    messages = message.split("\n")
    for message in messages:
        if message == "\n":
            logger.debug("")
        else:
            logger.debug(message)

def get_logger():
    return logging.getLogger("flypipe")

def config_logging(debug: bool = False):
    log_level = logging.WARNING
    if debug:
        log_level = logging.DEBUG

    class ColoredFormatter(logging.Formatter):
        """Custom formatter with colors and emojis for different log levels"""

        # ANSI color codes
        RED = "\033[91m"
        YELLOW = "\033[93m"
        BLUE = "\033[94m"
        RESET = "\033[0m"

        # Emojis for each level
        EMOJIS = {"INFO": "ℹ️", "WARNING": "⚠️", "ERROR": "🔴", "CRITICAL": "🚨"}

        def format(self, record):
            # Get emoji for the level
            emoji = self.EMOJIS.get(record.levelname, "")

            # Add color based on log level
            if record.levelno >= logging.ERROR:
                # Red for ERROR and CRITICAL
                message = f"{self.RED}{emoji} [Flypipe:{self.formatTime(record, '%Y-%m-%d %H:%M:%S')}] {record.getMessage()}{self.RESET}"
            elif record.levelno >= logging.WARNING:
                # Yellow for WARNING
                message = f"{self.YELLOW}{emoji} [Flypipe:{self.formatTime(record, '%Y-%m-%d %H:%M:%S')}] {record.getMessage()}{self.RESET}"
            elif record.levelno >= logging.INFO:
                # Blue for INFO
                emoji_str = "" if not emoji else f"{emoji} "
                message = f"{self.BLUE}{emoji_str}[Flypipe:{self.formatTime(record, '%Y-%m-%d %H:%M:%S')}] {record.getMessage()}{self.RESET}"
            else:
                # No color for DEBUG
                message = (
                    ("" if not emoji else f"{emoji} ")
                    + f"[Flypipe:{self.formatTime(record, '%Y-%m-%d %H:%M:%S')}] {record.getMessage()}"
                )

            return message

    # Configure only the Flypipe logger namespace (not the root logger)
    # This prevents interfering with other libraries like Spark/py4j
    flypipe_logger = get_logger()
    flypipe_logger.setLevel(log_level)

    # Remove any existing handlers to avoid duplicates
    flypipe_logger.handlers.clear()

    # Add our custom handler
    handler = logging.StreamHandler()
    handler.setFormatter(ColoredFormatter())
    flypipe_logger.addHandler(handler)

    # Prevent propagation to root logger to avoid duplicate messages
    flypipe_logger.propagate = False
