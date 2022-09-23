import numpy as np
import pandas as pd
import pytest
from numpy import dtype
from pyspark.sql.types import DoubleType

from flypipe.data_type import Double
from flypipe.utils import get_schema


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark

    return spark


@pytest.fixture(scope="function")
def pandas_df():
    return pd.DataFrame(data={"double": [np.double(1)]})


@pytest.fixture(scope="function")
def pyspark_df(spark, pandas_df):
    return spark.createDataFrame(pandas_df)


@pytest.fixture(scope="function")
def pandas_on_spark_df(pyspark_df):
    return pyspark_df.to_pandas_on_spark()


class TestDouble:
    def test_double(self, pandas_df, pyspark_df, pandas_on_spark_df):
        columns = ["double"]
        type_ = Double()
        df_cast = type_.cast(pandas_df, columns)

        assert {
            "double": dtype("float64"),
        } == get_schema(df_cast)

        df_cast = type_.cast(pandas_on_spark_df, columns)
        assert {
            "double": dtype("float64"),
        } == get_schema(df_cast)

        df_cast = type_.cast(pyspark_df, columns)
        assert {
            "double": DoubleType(),
        } == get_schema(df_cast)
