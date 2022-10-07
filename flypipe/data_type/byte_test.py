import numpy as np
import pandas as pd
import pytest
from numpy import dtype
from pyspark.sql.types import ByteType

from flypipe.data_type import Byte
from flypipe.utils import get_schema, DataFrameType


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark

    return spark


@pytest.fixture(scope="function")
def pandas_df():
    return pd.DataFrame(data={"byte": [np.byte(1)]})


@pytest.fixture(scope="function")
def pyspark_df(spark, pandas_df):
    return spark.createDataFrame(pandas_df)


@pytest.fixture(scope="function")
def pandas_on_spark_df(pyspark_df):
    return pyspark_df.to_pandas_on_spark()


class TestByte:
    def test_byte(self, pandas_df, pyspark_df, pandas_on_spark_df):
        columns = ["byte"]
        type_ = Byte()
        df_cast = None

        for col in columns:
            df_cast = type_.cast(pandas_df, DataFrameType.PANDAS, col)

        assert {
            "byte": dtype("int8"),
        } == get_schema(df_cast)

        for col in columns:
            df_cast = type_.cast(pandas_on_spark_df, DataFrameType.PANDAS_ON_SPARK, col)
        assert {
            "byte": dtype("int8"),
        } == get_schema(df_cast)

        for col in columns:
            df_cast = type_.cast(pyspark_df, DataFrameType.PYSPARK, col)
        assert {
            "byte": ByteType(),
        } == get_schema(df_cast)
