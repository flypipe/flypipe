import numpy as np
import pandas as pd
import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType

from flypipe.converter.schema import SchemaConverter
from flypipe.data_type import String, Integer, Date, Decimals
from flypipe.schema.column import Column
from flypipe.schema.schema import Schema
from flypipe.utils import DataFrameType


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark

    return spark


@pytest.fixture(scope="function")
def pandas_df():
    return pd.DataFrame(data={"name": ["jose"],
                              "age": ["30"],
                              "balance": ["-30.11111"],
                              "birth": ["01011980"]})


@pytest.fixture(scope="function")
def pyspark_df(spark, pandas_df):
    return spark.createDataFrame(pandas_df)


@pytest.fixture(scope="function")
def pandas_on_spark_df(pyspark_df):
    return pyspark_df.to_pandas_on_spark()


class TestSchemaConverter:

    def test_convert_pandas(self, spark, pandas_df, pandas_on_spark_df, pyspark_df):

        schema = Schema(
            [
                Column("name", String()),
                Column("age", Integer()),
                Column("balance", Decimals(6,2)),
                Column("birth", Date(fmt="%m%d%Y")),
            ]
        )

        pandas_df_ = SchemaConverter.cast(pandas_df.copy(deep=True),
                                          DataFrameType.PANDAS, schema)

        assert pandas_df_.dtypes['name'] == np.dtype("O")
        assert pandas_df_.dtypes['age'] == np.int
        assert pandas_df_.dtypes['balance'] == np.float
        assert pandas_df_.dtypes['birth'] == np.dtype("datetime64[ns]")


        pandas_on_spark_df_ = SchemaConverter.cast(pandas_on_spark_df.copy(deep=True),
                                                   DataFrameType.PANDAS_ON_SPARK,
                                                   schema)
        assert pandas_on_spark_df_.dtypes['name'] == np.dtype("<U")
        assert pandas_on_spark_df_.dtypes['age'] == np.int
        assert pandas_on_spark_df_.dtypes['balance'] == np.float
        assert pandas_on_spark_df_.dtypes['birth'] == np.dtype("datetime64[ns]")

    def test_convert_pypsark(self, spark, pyspark_df):
        schema = Schema(
            [
                Column("name", String()),
                Column("age", Integer()),
                Column("balance", Decimals(6, 2)),
                Column("birth", Date(fmt="MMddyyyy")),
            ]
        )

        pyspark_df_ = SchemaConverter.cast(pyspark_df, DataFrameType.PYSPARK,
                                           schema)

        assert pyspark_df_.schema == StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("balance", DecimalType(6,2), True),
            StructField("birth", DateType(), True)
        ])

