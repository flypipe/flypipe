import decimal

import numpy as np
import pandas as pd
import pytest
from numpy import dtype
from pyspark.sql.types import DecimalType

from flypipe.data_type import Decimals
from flypipe.utils import get_schema


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark

    return spark


@pytest.fixture(scope="function")
def pandas_df():
    return pd.DataFrame(
        data={
            "decimal": [decimal.Decimal("3.489")],
        }
    )


@pytest.fixture(scope="function")
def pyspark_df(spark, pandas_df):
    return spark.createDataFrame(pandas_df)


@pytest.fixture(scope="function")
def pandas_on_spark_df(pyspark_df):
    return pyspark_df.to_pandas_on_spark()


class TestDecimals:
    def test_decimal(self, pandas_df, pyspark_df, pandas_on_spark_df):
        columns = ["decimal"]
        type_ = Decimals(precision=10, scale=2)

        df_cast = type_.cast(pandas_df, columns)
        assert {
            "decimal": dtype("float64"),
        } == get_schema(df_cast)
        assert df_cast.loc[0, "decimal"] == np.round(3.489, decimals=2)

        df_cast = type_.cast(pandas_on_spark_df, columns)
        assert {
            "decimal": dtype("float64"),
        } == get_schema(df_cast)
        assert df_cast.loc[0, "decimal"] == np.round(3.489, decimals=2)

        df_cast = type_.cast(pyspark_df, columns)
        assert {
            "decimal": DecimalType(precision=10, scale=2),
        } == get_schema(df_cast)
        assert df_cast.toPandas().loc[0, "decimal"] == decimal.Decimal("3.49")
