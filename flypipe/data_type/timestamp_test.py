from datetime import datetime

import pandas as pd
import pytest
from numpy import dtype
from pyspark.sql.types import StringType, TimestampType, DateType

from flypipe.data_type import Timestamp
from flypipe.utils import get_schema, DataFrameType


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark

    return spark


@pytest.fixture(scope="function")
def pandas_df():
    return pd.DataFrame(
        data={
            "date": [datetime(2022, 1, 1, 13, 5, 13)],
            "date_str": ["31-01-2022 13:05:15"],
        }
    )


@pytest.fixture(scope="function")
def pyspark_df(spark, pandas_df):
    return spark.createDataFrame(pandas_df)


@pytest.fixture(scope="function")
def pandas_on_spark_df(pyspark_df):
    return pyspark_df.to_pandas_on_spark()


class TestTimestamp:
    def test_date(self, pandas_df, pyspark_df, pandas_on_spark_df):




        columns = ["date"]
        type_ = Timestamp(fmt="%Y-%m-%d %H:%M:%S")

        df_cast = None

        for col in columns:
            df_cast = type_.cast(pandas_df, DataFrameType.PANDAS, col)
        assert {
            "date": dtype("<M8[ns]"),
            "date_str": dtype("O"),
        } == get_schema(df_cast)
        assert df_cast.loc[0, "date"] == datetime(2022, 1, 1, 13, 5, 13)

        for col in columns:
            df_cast = type_.cast(pandas_on_spark_df, DataFrameType.PANDAS_ON_SPARK, col)
        assert {
            "date": dtype("<M8[ns]"),
            "date_str": dtype("O"),
        } == get_schema(df_cast)
        assert df_cast.loc[0, "date"] == datetime(2022, 1, 1, 13, 5, 13)

        type_ = Timestamp(fmt="yyyy-MM-dd HH:mm:ss")
        for col in columns:
            df_cast = type_.cast(pyspark_df, DataFrameType.PYSPARK, col)
        assert {
            "date": TimestampType(),
            "date_str": StringType(),
        } == get_schema(df_cast)
        assert df_cast.toPandas().loc[0, "date"] == datetime(2022, 1, 1, 13, 5, 13)

    def test_date_str(self, pandas_df, pyspark_df, pandas_on_spark_df):
        columns = ["date_str"]
        type_ = Timestamp(fmt="%d-%m-%Y %H:%M:%S")

        df_cast = None

        for col in columns:
            df_cast = type_.cast(pandas_df, DataFrameType.PANDAS, col)
        assert {
            "date": dtype('<M8[ns]'),
            "date_str": dtype("<M8[ns]"),
        } == get_schema(df_cast)


        for col in columns:
            df_cast = type_.cast(pandas_on_spark_df, DataFrameType.PANDAS_ON_SPARK, col)
        assert {
            "date": dtype('<M8[ns]'),
            "date_str": dtype("<M8[ns]"),
        } == get_schema(df_cast)

        type_ = Timestamp(fmt="dd-MM-yyyy HH:mm:ss")
        for col in columns:
            df_cast = type_.cast(pyspark_df, DataFrameType.PYSPARK, col)
        assert {
            "date": TimestampType(),
            "date_str": TimestampType(),
        } == get_schema(df_cast)
