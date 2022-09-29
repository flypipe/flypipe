import pandas as pd
import pytest
from numpy import dtype
from pyspark.sql.types import StringType

from flypipe.data_type import String
from flypipe.utils import get_schema


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark

    return spark


@pytest.fixture(scope="function")
def pandas_df():
    return pd.DataFrame(data={"str": ["my string"], "int": [1]})


@pytest.fixture(scope="function")
def pyspark_df(spark, pandas_df):
    return spark.createDataFrame(pandas_df)


@pytest.fixture(scope="function")
def pandas_on_spark_df(pyspark_df):
    return pyspark_df.to_pandas_on_spark()


class TestString:
    def test_str(self, pandas_df, pyspark_df, pandas_on_spark_df):
        columns = ["str"]
        type_ = String()
        df_cast = type_.cast(pandas_df, columns)
        assert dtype("O") == get_schema(df_cast)["str"]

        df_cast = type_.cast(pandas_on_spark_df, columns)
        assert dtype("<U") == get_schema(df_cast)["str"]

        df_cast = type_.cast(pyspark_df, columns)
        assert StringType() == get_schema(df_cast)["str"]

    def test_int(self, pandas_df, pyspark_df, pandas_on_spark_df):
        columns = ["int"]
        type_ = String()
        df_cast = type_.cast(pandas_df, columns)

        assert dtype("O") == get_schema(df_cast)["int"]

        df_cast = type_.cast(pandas_on_spark_df, columns)
        assert dtype("<U") == get_schema(df_cast)["int"]

        df_cast = type_.cast(pyspark_df, columns)
        assert StringType() == get_schema(df_cast)["int"]
