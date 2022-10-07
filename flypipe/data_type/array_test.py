import datetime

import numpy as np
import pandas as pd
import pytest
from numpy import dtype
from pyspark.sql.types import ArrayType, IntegerType, DateType

from flypipe.data_type import Array, Integer, Date
from flypipe.data_type.array import ArrayContentCast
from flypipe.utils import get_schema, DataFrameType


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark

    return spark


@pytest.fixture(scope="function")
def pandas_df():
    return pd.DataFrame(
        data={
            "int": [[1, 2]],
            "date": [
                [
                    datetime.datetime(2022, 1, 1).date(),
                    datetime.datetime(2022, 2, 1).date(),
                ]
            ],
            "date_str": [["31-01-2022", "31-01-2023"]],
        }
    )


@pytest.fixture(scope="function")
def pyspark_df(spark, pandas_df):
    return spark.createDataFrame(pandas_df)


@pytest.fixture(scope="function")
def pandas_on_spark_df(pyspark_df):
    return pyspark_df.to_pandas_on_spark()


class TestArray:
    def test_int(self, pandas_df, pyspark_df, pandas_on_spark_df):
        columns = ["int"]
        type_ = Array(Integer())
        df_cast = None

        for col in columns:
            df_cast = type_.cast(pandas_df, DataFrameType.PANDAS, col)
        assert dtype("object") == get_schema(df_cast)["int"]

        for col in columns:
            df_cast = type_.cast(pandas_on_spark_df, DataFrameType.PANDAS_ON_SPARK, col)
        assert dtype("object") == get_schema(df_cast)["int"]

        for col in columns:
            df_cast = type_.cast(pyspark_df, DataFrameType.PYSPARK, col)
        assert ArrayType(IntegerType()) == get_schema(df_cast)["int"]

    def test_date(self, pandas_df, pyspark_df, pandas_on_spark_df):
        columns = ["date"]
        type_ = Array(Date())
        df_cast = None

        for col in columns:
            df_cast = type_.cast(pandas_df, DataFrameType.PANDAS, col)

        assert dtype("object") == get_schema(df_cast)["date"]
        assert isinstance(df_cast.loc[0, columns[0]][0], datetime.date)

        for col in columns:
            df_cast = type_.cast(pandas_on_spark_df, DataFrameType.PANDAS_ON_SPARK, col)
        assert dtype("object") == get_schema(df_cast)["date"]
        assert isinstance(df_cast.loc[0, columns[0]][0], np.str)

        for col in columns:
            df_cast = type_.cast(pyspark_df, DataFrameType.PYSPARK, col)
        assert ArrayType(DateType()) == get_schema(df_cast)["date"]

    def test_date_str(self, pandas_df, pyspark_df, pandas_on_spark_df):
        columns = ["date_str"]

        with pytest.warns(
            ArrayContentCast,
            match="Make sure the content of the array has been casted to the proper type",
        ):
            type_ = Array(Date(fmt="%d-%m-%Y"))

        df_cast = None
        for col in columns:
            df_cast = type_.cast(pandas_df, DataFrameType.PANDAS, col)

        assert dtype("object") == get_schema(df_cast)["date_str"]
        assert isinstance(df_cast.loc[0, columns[0]][0], np.str)

        for col in columns:
            df_cast = type_.cast(pandas_on_spark_df, DataFrameType.PANDAS_ON_SPARK, col)
        assert dtype("object") == get_schema(df_cast)["date_str"]

        for col in columns:
            df_cast = type_.cast(pyspark_df, DataFrameType.PYSPARK, col)
        assert ArrayType(DateType()) == get_schema(df_cast)["date_str"]
