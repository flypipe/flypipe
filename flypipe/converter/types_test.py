from decimal import Decimal as DecimalPy

import pandas as pd
import pytest
from numpy import dtype
import numpy as np
from datetime import datetime

from pyspark.sql.types import BooleanType, StringType, LongType, DoubleType, DecimalType

from flypipe.converter.types import Boolean, DateType, Date, Decimal
from flypipe.exceptions import ErrorColumnNotInDataframe
from flypipe.utils import get_schema


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark

    return spark


@pytest.fixture(scope="function")
def pandas_df():
    return pd.DataFrame(
        data={
            "bool": [True],
            "int": [1],
            "date": [datetime(2022, 1, 1).date()],
            "date_str": ["31-01-2022"],
            "decimal": DecimalPy("3.489"),
        }
    )


@pytest.fixture(scope="function")
def pyspark_df(spark, pandas_df):
    return spark.createDataFrame(pandas_df)


@pytest.fixture(scope="function")
def pandas_on_spark_df(pyspark_df):
    return pyspark_df.to_pandas_on_spark()


class TestTypes:
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
            "date": dtype("O"),
            "date_str": dtype("O"),
            "int": dtype("int64"),
            "decimal": dtype("O"),
        } == get_schema(df_cast)

        columns = ["bool", "int"]
        type_ = Boolean()
        df_cast = type_.cast(pandas_df, columns)
        assert {
            "bool": dtype("bool"),
            "date": dtype("O"),
            "date_str": dtype("O"),
            "int": dtype("bool"),
            "decimal": dtype("O"),
        } == get_schema(df_cast)

        df_cast = type_.cast(pandas_on_spark_df, columns)
        assert {
            "bool": dtype("bool"),
            "date": dtype("O"),
            "date_str": dtype("O"),
            "int": dtype("bool"),
            "decimal": dtype("O"),
        } == get_schema(df_cast)

        df_cast = type_.cast(pyspark_df, columns)
        assert {
            "bool": BooleanType(),
            "date": DateType(),
            "date_str": StringType(),
            "int": BooleanType(),
            "decimal": DecimalType(4, 3),
        } == get_schema(df_cast)

    def test_date(self, pandas_df, pyspark_df, pandas_on_spark_df):
        columns = ["date"]
        type_ = Date()

        df_cast = type_.cast(pandas_df, columns)
        assert {
            "bool": dtype("bool"),
            "date": dtype("<M8[ns]"),
            "date_str": dtype("O"),
            "int": dtype("int64"),
            "decimal": dtype("O"),
        } == get_schema(df_cast)
        assert df_cast.loc[0, "date"] == datetime(2022, 1, 1).date()

        df_cast = type_.cast(pandas_on_spark_df, columns)
        assert {
            "bool": dtype("bool"),
            "date": dtype("<M8[ns]"),
            "date_str": dtype("O"),
            "int": dtype("int64"),
            "decimal": dtype("O"),
        } == get_schema(df_cast)
        assert df_cast.loc[0, "date"] == datetime(2022, 1, 1).date()

        df_cast = type_.cast(pyspark_df, columns)
        assert {
            "bool": BooleanType(),
            "date": DateType(),
            "date_str": StringType(),
            "int": LongType(),
            "decimal": DecimalType(4, 3),
        } == get_schema(df_cast)
        assert df_cast.toPandas().loc[0, "date"] == datetime(2022, 1, 1).date()

    def test_date_str(self, pandas_df, pyspark_df, pandas_on_spark_df):
        columns = ["date_str"]
        type_ = Date(fmt="%d-%m-%Y")

        df_cast = type_.cast(pandas_df, columns)
        assert {
            "bool": dtype("bool"),
            "date": dtype("O"),
            "date_str": dtype("<M8[ns]"),
            "int": dtype("int64"),
            "decimal": dtype("O"),
        } == get_schema(df_cast)
        assert df_cast.loc[0, "date_str"] == datetime(2022, 1, 31).date()

        df_cast = type_.cast(pandas_on_spark_df, columns)
        assert {
            "bool": dtype("bool"),
            "date": dtype("O"),
            "date_str": dtype("<M8[ns]"),
            "int": dtype("int64"),
            "decimal": dtype("O"),
        } == get_schema(df_cast)
        assert df_cast.loc[0, "date_str"] == datetime(2022, 1, 31).date()

        type_ = Date(fmt="dd-MM-yyyy")
        df_cast = type_.cast(pyspark_df, columns)

        assert {
            "bool": BooleanType(),
            "date": DateType(),
            "date_str": DateType(),
            "int": LongType(),
            "decimal": DecimalType(4, 3),
        } == get_schema(df_cast)
        assert df_cast.toPandas().loc[0, "date_str"] == datetime(2022, 1, 31).date()

    def test_decimal(self, pandas_df, pyspark_df, pandas_on_spark_df):
        columns = ["decimal"]
        type_ = Decimal(precision=10, scale=2)

        df_cast = type_.cast(pandas_df, columns)
        assert {
            "bool": dtype("bool"),
            "date": dtype("O"),
            "date_str": dtype("O"),
            "int": dtype("int64"),
            "decimal": dtype("float64"),
        } == get_schema(df_cast)
        assert df_cast.loc[0, "decimal"] == np.round(3.489, decimals=2)

        df_cast = type_.cast(pandas_on_spark_df, columns)
        assert {
            "bool": dtype("bool"),
            "date": dtype("O"),
            "date_str": dtype("O"),
            "int": dtype("int64"),
            "decimal": dtype("float64"),
        } == get_schema(df_cast)
        assert df_cast.loc[0, "decimal"] == np.round(3.489, decimals=2)

        df_cast = type_.cast(pyspark_df, columns)
        assert {
            "bool": BooleanType(),
            "date": DateType(),
            "date_str": StringType(),
            "int": LongType(),
            "decimal": DecimalType(precision=10, scale=2),
        } == get_schema(df_cast)
        assert df_cast.toPandas().loc[0, "decimal"] == DecimalPy("3.49")
