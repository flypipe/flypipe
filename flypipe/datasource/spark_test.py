import pytest
from pandas._testing import assert_frame_equal

from flypipe import Spark, ErrorTimeTravel
from tests.utils.spark import spark
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DateType

data = [("id1", datetime.strptime("2022-01-01", "%Y-%m-%d")),
        ("id2", datetime.strptime("2022-01-02", "%Y-%m-%d")),
        ("id3", datetime.strptime("2022-01-03", "%Y-%m-%d")),
        ("id4", datetime.strptime("2022-01-04", "%Y-%m-%d")),
        ("id5", datetime.strptime("2022-01-05", "%Y-%m-%d"))]

schema = StructType([
    StructField("id", StringType(), True),
    StructField("date", DateType(), True)
])

df = spark.createDataFrame(data=data, schema=schema)
df.createOrReplaceTempView("spark_datasource")


class TestSparkDataSource():

    def test_load_filter_first_date(self):
        df = Spark(spark).load("spark_datasource",
                               time_travel_column="date",
                               start_time_travel="2022-01-04")

        data = [("id4", datetime.strptime("2022-01-04", "%Y-%m-%d")),
                ("id5", datetime.strptime("2022-01-05", "%Y-%m-%d"))]
        df_expected = spark.createDataFrame(data=data, schema=schema)
        assert_frame_equal(df_expected.toPandas(), df.toPandas())

    def test_load_filter_end_date(self):
        df = Spark(spark).load("spark_datasource",
                               time_travel_column="date",
                               end_time_travel="2022-01-02")

        data = [("id1", datetime.strptime("2022-01-01", "%Y-%m-%d")),
                ("id2", datetime.strptime("2022-01-02", "%Y-%m-%d"))]
        df_expected = spark.createDataFrame(data=data, schema=schema)
        assert_frame_equal(df_expected.toPandas(), df.toPandas())

    def test_load_filter_two_dates(self):
        df = Spark(spark).load("spark_datasource",
                               time_travel_column="date",
                               start_time_travel="2022-01-02",
                               end_time_travel="2022-01-04")

        data = [("id2", datetime.strptime("2022-01-02", "%Y-%m-%d")),
                ("id3", datetime.strptime("2022-01-03", "%Y-%m-%d")),
                ("id4", datetime.strptime("2022-01-04", "%Y-%m-%d"))]
        df_expected = spark.createDataFrame(data=data, schema=schema)
        assert_frame_equal(df_expected.toPandas(), df.toPandas())

    def test_load_timetravel(self):
        data = [("id1", datetime.strptime("2022-01-01", "%Y-%m-%d"))]

        schema = StructType([
            StructField("id", StringType(), True),
            StructField("date", DateType(), True)
        ])

        df = spark.createDataFrame(data=data, schema=schema)
        df.createOrReplaceTempView("spark_datasource")

        with pytest.raises(ErrorTimeTravel):
            Spark(spark).load("spark_datasource", start_time_travel="2022-01-02")

        with pytest.raises(ErrorTimeTravel):
            Spark(spark).load("spark_datasource", end_time_travel="2022-01-02")
