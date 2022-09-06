import pandas as pd
import pytest
import os

from flypipe.datasource.csv import (
    CSV,
    ErrorModeNotSupported,
    ErrorDataFrameTypeNotSupported,
)
from flypipe.mode import Mode
from flypipe.utils import assert_dataframes_equals


@pytest.fixture
def csv_path():
    try:
        os.unlink("datasource_csv.csv")
    except:
        pass

    os.path.join(os.getcwd(), "datasource_csv.csv")
    pd.DataFrame(data={"a": ["1a"], "b": [1]}).to_csv(
        open("datasource_csv.csv", "w"), index=False
    )
    yield "datasource_csv.csv"
    os.unlink("datasource_csv.csv")


@pytest.fixture
def spark():
    from tests.utils.spark import spark

    return spark


class TestCSV:
    def test_exception_mode_not_supported(self, csv_path):
        with pytest.raises(ErrorModeNotSupported):
            CSV.load(csv_path, mode=Mode.SPARK_SQL)

    def test_exception_spark_session_not_provided(self, csv_path):
        with pytest.raises(ErrorDataFrameTypeNotSupported):
            CSV.load(csv_path, mode=Mode.PYSPARK)

        with pytest.raises(ErrorDataFrameTypeNotSupported):
            CSV.load(csv_path, mode=Mode.PANDAS_ON_SPARK)

    def test_pandas(self, csv_path):
        import pandas as pd

        df = CSV.load(csv_path, mode=Mode.PANDAS)

        expected_df = pd.read_csv(csv_path)
        assert_dataframes_equals(expected_df, df)

    def test_spark(self, csv_path, spark):
        df = CSV.load(csv_path, spark=spark, mode=Mode.PYSPARK)

        expected_df = spark.read.option("header", True).csv(csv_path)
        assert_dataframes_equals(df, expected_df)

    def test_pandas_on_spark(self, csv_path, spark):
        df = CSV.load(csv_path, spark=spark, mode=Mode.PANDAS_ON_SPARK)

        expected_df = (
            spark.read.option("header", True).csv(csv_path).to_pandas_on_spark()
        )
        assert_dataframes_equals(df, expected_df)
