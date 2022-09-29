import numpy as np
import pandas as pd
import pytest
from numpy import dtype
from pyspark.sql.types import ShortType

from flypipe.data_type import Short
from flypipe.utils import get_schema


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark

    return spark


@pytest.fixture(scope="function")
def pandas_df():
    return pd.DataFrame(data={"short": [np.short(1)]})


@pytest.fixture(scope="function")
def pyspark_df(spark, pandas_df):
    return spark.createDataFrame(pandas_df)


@pytest.fixture(scope="function")
def pandas_on_spark_df(pyspark_df):
    return pyspark_df.to_pandas_on_spark()


class TestShort:
    def test_short(self, pandas_df, pyspark_df, pandas_on_spark_df):
        columns = ["short"]
        type_ = Short()
        df_cast = type_.cast(pandas_df, columns)

        assert {
            "short": dtype("int16"),
        } == get_schema(df_cast)

        df_cast = type_.cast(pandas_on_spark_df, columns)
        assert {
            "short": dtype("int16"),
        } == get_schema(df_cast)

        df_cast = type_.cast(pyspark_df, columns)
        assert {
            "short": ShortType(),
        } == get_schema(df_cast)
