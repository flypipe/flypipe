import json
from enum import Enum

import pandas as pd
import pyspark.pandas as ps
import pyspark.sql.dataframe as sql
from pandas.testing import assert_frame_equal

from flypipe.exceptions import (
    DataframeDifferentDataError,
    DataframeSchemasDoNotMatchError,
    DataframeTypeNotSupportedError,
)


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
    if isinstance(df, sql.DataFrame):
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
