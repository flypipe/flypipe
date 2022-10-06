import numpy as np
import pandas as pd
import pytest
from numpy import dtype
from pyspark.sql.types import BinaryType

from flypipe.data_type import Binary
from flypipe.utils import get_schema


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark

    return spark


@pytest.fixture(scope="function")
def pandas_df():
    return pd.DataFrame(data={"binary": ["a".encode("utf-8")]}).astype(np.bytes_)


@pytest.fixture(scope="function")
def pyspark_df(spark, pandas_df):
    return spark.createDataFrame(pandas_df)


@pytest.fixture(scope="function")
def pandas_on_spark_df(pyspark_df):
    return pyspark_df.to_pandas_on_spark()


class TestBinary:
    def test_binary(self, pandas_df, pyspark_df, pandas_on_spark_df):
        columns = ["binary"]
        type_ = Binary()
        df_cast = type_.cast(pandas_df, columns)

        assert {
            "binary": dtype("S1"),
        } == get_schema(df_cast)

        df_cast = type_.cast(pandas_on_spark_df, columns)
        assert {
            "binary": dtype("S"),
        } == get_schema(df_cast)

        df_cast = type_.cast(pyspark_df, columns)
        assert {
            "binary": BinaryType(),
        } == get_schema(df_cast)
