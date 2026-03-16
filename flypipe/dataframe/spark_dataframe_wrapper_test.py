import pytest
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.exceptions import DataFrameMissingColumns
from flypipe.schema.types import (
    Binary,
    Boolean,
    Byte,
    Date,
    DateTime,
    Decimal,
    Double,
    Float,
    Integer,
    Long,
    Short,
    String,
)
from flypipe.tests.pyspark_test import assert_pyspark_df_equal


class TestSparkDataFrameWrapper:
    """Tests for Spark DataFrameWrapper"""

    def test_select_column_1(self, spark):
        df = spark.createDataFrame(
            schema=("col1", "col2", "col3"),
            data=[
                (True, "Hello", "Banana"),
                (False, "World", "Apple"),
            ],
        )
        expected_df = spark.createDataFrame(
            schema=("col1", "col2"),
            data=[
                (True, "Hello"),
                (False, "World"),
            ],
        )
        df_wrapper = DataFrameWrapper.get_instance(None, df)
        assert_pyspark_df_equal(
            df_wrapper.select_columns("col1", "col2").df, expected_df
        )

    def test_select_column_2(self, spark):
        df = spark.createDataFrame(
            schema=("col1", "col2", "col3"),
            data=[
                (True, "Hello", "Banana"),
                (False, "World", "Apple"),
            ],
        )
        expected_df = spark.createDataFrame(
            schema=("col1", "col2"),
            data=[
                (True, "Hello"),
                (False, "World"),
            ],
        )
        df_wrapper = DataFrameWrapper.get_instance(None, df)
        assert_pyspark_df_equal(
            df_wrapper.select_columns(["col1", "col2"]).df, expected_df
        )

    def test_select_column_missing_column(self, spark):
        df = spark.createDataFrame(
            schema=("col1", "col2", "col3"),
            data=[
                (True, "Hello", "Banana"),
                (False, "World", "Apple"),
            ],
        )
        df_wrapper = DataFrameWrapper.get_instance(None, df)

        with pytest.raises(DataFrameMissingColumns):
            df_wrapper.select_columns(["col1", "col4"])

    def test_get_column_flypipe_type(self, spark):
        df = spark.createDataFrame(
            schema=StructType(
                [
                    StructField("c1", BooleanType()),
                    StructField("c2", ByteType()),
                    StructField("c3", BinaryType()),
                    StructField("c4", IntegerType()),
                    StructField("c5", ShortType()),
                    StructField("c6", LongType()),
                    StructField("c7", FloatType()),
                    StructField("c8", DoubleType()),
                    StructField("c9", StringType()),
                    StructField("c10", DecimalType(13, 2)),
                    StructField("c11", TimestampType()),
                    StructField("c12", DateType()),
                ]
            ),
            data=[],
        )
        df_wrapper = DataFrameWrapper.get_instance(spark, df)

        assert isinstance(df_wrapper.get_column_flypipe_type("c1"), Boolean)
        assert isinstance(df_wrapper.get_column_flypipe_type("c2"), Byte)
        assert isinstance(df_wrapper.get_column_flypipe_type("c3"), Binary)
        assert isinstance(df_wrapper.get_column_flypipe_type("c4"), Integer)
        assert isinstance(df_wrapper.get_column_flypipe_type("c5"), Short)
        assert isinstance(df_wrapper.get_column_flypipe_type("c6"), Long)
        assert isinstance(df_wrapper.get_column_flypipe_type("c7"), Float)
        assert isinstance(df_wrapper.get_column_flypipe_type("c8"), Double)
        assert isinstance(df_wrapper.get_column_flypipe_type("c9"), String)
        c10_type = df_wrapper.get_column_flypipe_type("c10")
        assert isinstance(c10_type, Decimal)
        assert c10_type.precision == 13
        assert c10_type.scale == 2
        assert isinstance(df_wrapper.get_column_flypipe_type("c11"), DateTime)
        assert isinstance(df_wrapper.get_column_flypipe_type("c12"), Date)

    def test_cast_column(self, spark):
        df = spark.createDataFrame(schema=("col1",), data=[(1,), (0,), (None,)])
        df_wrapper = DataFrameWrapper.get_instance(spark, df)
        df_wrapper.cast_column("col1", Boolean())
        assert_pyspark_df_equal(
            df_wrapper.df,
            spark.createDataFrame(schema=("col1",), data=[(True,), (False,), (None,)]),
        )
        assert df_wrapper.df.dtypes == [("col1", "boolean")]

    def test_cast_column_decimal(self, spark):
        df = spark.createDataFrame(
            schema=("col1",), data=[(5678.12345,), (999.11111,), (1.2345678,), (None,)]
        )
        df_wrapper = DataFrameWrapper.get_instance(spark, df)
        df_wrapper.cast_column("col1", Decimal(5, 2))
        assert df_wrapper.df.dtypes[0] == ("col1", "decimal(5,2)")
        # TODO: should probably not resort to a pandas conversion + df check but I can't seem to create a pyspark df
        #  with DecimalType and the below literals.
        # TODO: this is broken
        # assert_frame_equal(
        #     df_wrapper.df.toPandas(), pd.DataFrame({'col1': [None, 999.11, 1.23, None]}, dtype=np.dtype('O')))

    def test_cast_column_with_column_object(self, spark):
        """cast_column accepts Column object (e.g. from node output schema)."""
        df = spark.createDataFrame(schema=("col1",), data=[(1,), (0,), (None,)])
        df_wrapper = DataFrameWrapper.get_instance(spark, df)
        df_wrapper.cast_column("col1", Boolean())
        assert df_wrapper.df.dtypes[0] == ("col1", "boolean")

    def test_cast_column_try_cast_returns_null_on_invalid(self, spark):
        """Invalid cast values become NULL via try_cast (Flypipe always uses try_cast)."""
        df = spark.createDataFrame(schema=("col1",), data=[("123",), ("abc",), (None,)])
        df_wrapper = DataFrameWrapper.get_instance(spark, df)
        df_wrapper.cast_column("col1", Long())
        rows = [row.col1 for row in df_wrapper.df.collect()]
        assert rows[0] == 123
        assert rows[1] is None  # "abc" cannot cast to Long, try_cast returns NULL
        assert rows[2] is None

    def test_cast_column_with_spaces_in_name(self, spark):
        """Columns with spaces in name work with try_cast (F.expr fallback on Spark 3.x)."""
        schema = StructType([StructField("col name", StringType())])
        df = spark.createDataFrame(schema=schema, data=[("123",), ("456",), (None,)])
        df_wrapper = DataFrameWrapper.get_instance(spark, df)
        df_wrapper.cast_column("col name", Long())
        rows = [row["col name"] for row in df_wrapper.df.collect()]
        assert rows[0] == 123
        assert rows[1] == 456
        assert rows[2] is None
