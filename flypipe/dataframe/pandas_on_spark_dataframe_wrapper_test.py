import pandas as pd
import pyspark.pandas as ps
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql.types import (
    StructField,
    StructType,
    BooleanType,
)

from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.exceptions import DataFrameMissingColumns
from flypipe.schema.types import (
    Boolean,
)


class TestPandasOnSparkDataFrameWrapper:
    """Tests for PandasOnSpark"""

    def test_select_column_1(self):
        df = ps.DataFrame(
            {
                "col1": [True, False],
                "col2": ["Hello", "World"],
                "col3": ["Banana", "Apple"],
            }
        )
        expected_df = pd.DataFrame(
            {
                "col1": [True, False],
                "col2": ["Hello", "World"],
            }
        )
        df_wrapper = DataFrameWrapper.get_instance(None, df)
        assert_frame_equal(
            df_wrapper.select_columns("col1", "col2").df.to_pandas(), expected_df
        )

    def test_select_column_2(self):
        df = ps.DataFrame(
            {
                "col1": [True, False],
                "col2": ["Hello", "World"],
                "col3": ["Banana", "Apple"],
            }
        )
        expected_df = pd.DataFrame(
            {
                "col1": [True, False],
                "col2": ["Hello", "World"],
            }
        )
        df_wrapper = DataFrameWrapper.get_instance(None, df)
        assert_frame_equal(
            df_wrapper.select_columns(["col1", "col2"]).df.to_pandas(), expected_df
        )

    def test_select_column_missing_column(self):
        df = ps.DataFrame(
            {
                "col1": [True, False],
                "col2": ["Hello", "World"],
                "col3": ["Banana", "Apple"],
            }
        )
        df_wrapper = DataFrameWrapper.get_instance(None, df)

        with pytest.raises(DataFrameMissingColumns):
            df_wrapper.select_columns(["col1", "col4"])

    def test_get_column_flypipe_type(self, spark):
        df = spark.createDataFrame(
            schema=StructType(
                [
                    StructField("c1", BooleanType()),
                ]
            ),
            data=[],
        ).pandas_api()
        df_wrapper = DataFrameWrapper.get_instance(spark, df)

        assert isinstance(df_wrapper.get_column_flypipe_type("c1"), Boolean)
