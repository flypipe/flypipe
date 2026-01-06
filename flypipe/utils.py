import json
import logging
import sys
from enum import Enum

from pandas.testing import assert_frame_equal
import pandas as pd


# Sparkleframe detection and conditional imports
def sparkleframe_is_active() -> bool:
    """
    Check if sparkleframe activation is enabled.
    
    Sparkleframe is a Polars-based implementation of the PySpark API.
    When active, it replaces PySpark's implementation with Polars underneath.
    
    Returns
    -------
    bool
        True if sparkleframe is active (PySpark module is actually sparkleframe),
        False otherwise (regular PySpark or PySpark not installed)
    """
    try:
        from pyspark.sql import SparkSession
        
        spark_session_module = SparkSession.__module__.split(".")[0]
        return spark_session_module == "sparkleframe"
    except ImportError:
        # PySpark (or sparkleframe) not installed
        return False


# Conditional imports based on sparkleframe activation
try:
    import pyspark.sql.dataframe as sql
    import sparkleframe.polarsdf.dataframe as sparkle_dataframe

    if sparkleframe_is_active():
        # If using sparkleframe activate, it will fail because they do not implement pyspark.pandas
        import pandas as ps
        
        # If using sparkleframe activate, it will fail because they do not implement pyspark.sql.connect
        import pyspark.sql.dataframe as sql_connect
    else:
        import pyspark.pandas as ps
        import pyspark.sql.connect.dataframe as sql_connect
except ImportError:
    # PySpark/Sparkleframe not installed - these will be None
    ps = None
    sql = None
    sql_connect = None
    sparkle_dataframe = None

# Snowpark imports (independent of PySpark)
try:
    import snowflake.snowpark.dataframe as snowpark_dataframe
except ImportError:
    snowpark_dataframe = None


class DataFrameType(Enum):
    """
    Enum of possible dataframe types
    """

    PANDAS = "pandas"
    PANDAS_ON_SPARK = "pandas_on_spark"
    PYSPARK = "pyspark"
    SNOWPARK = "snowpark"


def assert_schemas_are_equals(df1, df2) -> None:

    # Avoid Circular Reference
    from flypipe.exceptions import DataframeSchemasDoNotMatchError

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

    # Avoid Circular Reference
    from flypipe.exceptions import DataframeDifferentDataError

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
    """
    Determine the type of DataFrame.
    
    Parameters
    ----------
    df : DataFrame
        A DataFrame object from any supported library
    
    Returns
    -------
    DataFrameType
        The type of the DataFrame
    
    Raises
    ------
    DataframeTypeNotSupportedError
        If the DataFrame type is not supported
    """
    # Avoid Circular Reference
    from flypipe.exceptions import DataframeTypeNotSupportedError

    # Check Pandas first (always available)
    if isinstance(df, pd.DataFrame):
        return DataFrameType.PANDAS
    
    # Check PySpark types (if available)
    if ps is not None and isinstance(df, ps.DataFrame):
        return DataFrameType.PANDAS_ON_SPARK
    
    if (
        (sql is not None and isinstance(df, sql.DataFrame))
        or (sql_connect is not None and isinstance(df, sql_connect.DataFrame))
        or (sparkle_dataframe is not None and isinstance(df, sparkle_dataframe.DataFrame))
    ):
        return DataFrameType.PYSPARK
    
    # Check Snowpark types (if available)
    if snowpark_dataframe is not None and isinstance(df, snowpark_dataframe.DataFrame):
        return DataFrameType.SNOWPARK
    
    # Provide helpful error messages for missing dependencies
    error_msg = f"Unsupported DataFrame type: {type(df)}"
    
    # Check if it might be a PySpark DataFrame without PySpark installed
    if "pyspark" in str(type(df)).lower():
        error_msg += "\n\nPySpark is not installed. Install it with: pip install flypipe[spark]"
    
    # Check if it might be a Snowpark DataFrame without Snowflake installed
    elif "snowpark" in str(type(df)).lower():
        error_msg += "\n\nSnowflake Snowpark is not installed. Install it with: pip install flypipe[snowflake]"
    
    raise DataframeTypeNotSupportedError(error_msg)


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


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors and emojis for different log levels."""

    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    RESET = "\033[0m"

    EMOJIS = {
        logging.INFO: "ℹ️",
        logging.WARNING: "⚠️",
        logging.ERROR: "🔴",
        logging.CRITICAL: "🚨",
    }

    def format(self, record: logging.LogRecord) -> str:
        emoji = self.EMOJIS.get(record.levelno, "")
        timestamp = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
        name = record.name

        lines = record.getMessage().split("\n")
        formatted_lines: list[str] = []

        for line in lines:

            messages = ["" if msg == "\n" else msg for msg in line.split("\n")]
            for message in messages:

                prefix = f"[{name}:{timestamp}] {message}"

                if record.levelno >= logging.ERROR:
                    msg = f"{self.RED}{emoji} {prefix}{self.RESET}"
                elif record.levelno >= logging.WARNING:
                    msg = f"{self.YELLOW}{emoji} {prefix}{self.RESET}"
                elif record.levelno >= logging.INFO:
                    emoji_str = f"{emoji} " if emoji else ""
                    msg = f"{self.BLUE}{emoji_str}{prefix}{self.RESET}"
                else:
                    emoji_str = f"{emoji} " if emoji else ""
                    msg = f"{emoji_str}{prefix}"

                formatted_lines.append(msg)

        return "\n".join(formatted_lines)


def get_logger(
    logger_name: str = "Flypipe",
    log_level: int = logging.DEBUG,
    enabled: bool = True,
) -> logging.Logger:
    """
    Get a named logger, configuring it lazily on first use.

    Parameters
    ----------
    logger_name : str
        Name of the logger (default: "Flypipe")
    log_level : int
        Logging level (e.g. logging.DEBUG) (default: logging.DEBUG)
    enabled : bool
        If False, disable all logging output (default: True)
    """
    logger = logging.getLogger(logger_name)

    # Configure only once per logger name
    if not logger.handlers:
        # If disabled, set level to CRITICAL + 1 to suppress all logs
        effective_level = log_level if enabled else logging.WARNING
        logger.setLevel(effective_level)

        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setLevel(effective_level)
        handler.setFormatter(ColoredFormatter())

        logger.addHandler(handler)
        logger.propagate = False
    else:
        # Update level if logger already exists
        effective_level = log_level if enabled else logging.WARNING
        logger.setLevel(effective_level)
        for handler in logger.handlers:
            handler.setLevel(effective_level)

    return logger
