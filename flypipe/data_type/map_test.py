import datetime

import pandas as pd
import pytest
from numpy import dtype
from pyspark.sql.types import MapType, IntegerType, StringType, DateType

from flypipe.data_type import Map, Integer, String, Date
from flypipe.data_type.map import MapContentCast
from flypipe.utils import get_schema


# pytestmark = pytest.mark.filterwarnings("error")


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark

    return spark


@pytest.fixture(scope="function")
def pandas_df():
    return pd.DataFrame(
        data={
            "int_str": [{1: "my_test"}],
            "str_date": [{"date_1": datetime.datetime(2022, 1, 1).date()}],
            "int_str_str": [{1: {"my_test": "my_test2"}}],
        }
    )


@pytest.fixture(scope="function")
def pyspark_df(spark, pandas_df):
    return spark.createDataFrame(pandas_df)


@pytest.fixture(scope="function")
def pandas_on_spark_df(pyspark_df):
    return pyspark_df.to_pandas_on_spark()


class TestMap:
    def test_int_str(self, pandas_df, pyspark_df, pandas_on_spark_df):
        columns = ["int_str"]
        with pytest.warns(
            MapContentCast,
            match="Make sure the content of the map has been casted to the proper key and value types",
        ):
            type_ = Map(Integer(), String())

        df_cast = type_.cast(pandas_df, columns)

        assert dtype("O") == get_schema(df_cast)["int_str"]

        df_cast = type_.cast(pandas_on_spark_df, columns)
        assert dtype("O") == get_schema(df_cast)["int_str"]

        df_cast = type_.cast(pyspark_df, columns)
        assert MapType(IntegerType(), StringType()) == get_schema(df_cast)["int_str"]

    def test_str_date(self, pandas_df, pyspark_df, pandas_on_spark_df):
        columns = ["str_date"]
        with pytest.warns(
            MapContentCast,
            match="Make sure the content of the map has been casted to the proper key and value types",
        ):
            type_ = Map(String(), Date())

        df_cast = type_.cast(pandas_df, columns)

        assert dtype("O") == get_schema(df_cast)["str_date"]

        df_cast = type_.cast(pandas_on_spark_df, columns)
        assert dtype("O") == get_schema(df_cast)["str_date"]

        df_cast = type_.cast(pyspark_df, columns)
        assert MapType(StringType(), DateType()) == get_schema(df_cast)["str_date"]

    def test_int_str_str(self, pandas_df, pyspark_df, pandas_on_spark_df):
        columns = ["int_str_str"]
        with pytest.warns(
            MapContentCast,
            match="Make sure the content of the map has been casted to the proper key and value types",
        ):
            type_ = Map(Integer(), Map(String(), String()))

        df_cast = type_.cast(pandas_df, columns)

        assert dtype("O") == get_schema(df_cast)["int_str_str"]

        df_cast = type_.cast(pandas_on_spark_df, columns)
        assert dtype("O") == get_schema(df_cast)["int_str_str"]

        df_cast = type_.cast(pyspark_df, columns)
        assert MapType(IntegerType(), MapType(StringType(), StringType())) == get_schema(df_cast)["int_str_str"]