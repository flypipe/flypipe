import pandas as pd
import pytest
from numpy import dtype
from pyspark.sql.types import BooleanType

from flypipe.schema import Boolean
from flypipe.exceptions import ErrorColumnNotInDataframe
from flypipe.utils import get_schema


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark

    return spark


@pytest.fixture(scope="function")
def pandas_df():
    return pd.DataFrame(data={"bool": [True], "int": [1]})


@pytest.fixture(scope="function")
def pyspark_df(spark, pandas_df):
    return spark.createDataFrame(pandas_df)


@pytest.fixture(scope="function")
def pandas_on_spark_df(pyspark_df):
    return pyspark_df.to_pandas_on_spark()


class TestBoolean:
    def test_column_exists(self, pandas_df, pyspark_df, pandas_on_spark_df):
        columns = ["non_existent_colum"]
        type_ = Boolean()
        with pytest.raises(ErrorColumnNotInDataframe):
            type_.cast(pandas_df, columns)
            type_.cast(pyspark_df, columns)
            type_.cast(pandas_on_spark_df, columns)

    def test_boolean(self, pandas_df, pyspark_df, pandas_on_spark_df):
        columns = ["bool"]
        type_ = Boolean()
        df_cast = type_.cast(pandas_df, columns)
        assert {
            "bool": dtype("bool"),
            "int": dtype("int64"),
        } == get_schema(df_cast)

        columns = ["bool", "int"]
        type_ = Boolean()
        df_cast = type_.cast(pandas_df, columns)
        assert {
            "bool": dtype("bool"),
            "int": dtype("bool"),
        } == get_schema(df_cast)

        df_cast = type_.cast(pandas_on_spark_df, columns)
        assert {
            "bool": dtype("bool"),
            "int": dtype("bool"),
        } == get_schema(df_cast)

        df_cast = type_.cast(pyspark_df, columns)
        assert {
            "bool": BooleanType(),
            "int": BooleanType(),
        } == get_schema(df_cast)
