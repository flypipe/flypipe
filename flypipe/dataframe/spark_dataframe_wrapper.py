import pyspark.sql.functions as F
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
    Type,
    Unknown,
)
from flypipe.utils import DataFrameType


class SparkDataFrameWrapper(DataFrameWrapper):
    """
    Wrapper around a Spark dataframe. This gives some conversion functionality between Flypipe types and their spark
    equivalents.
    """

    DF_TYPE = DataFrameType.PYSPARK
    FLYPIPE_TYPE_TO_DF_TYPE_MAP = {
        Boolean.key(): BooleanType(),
        Byte.key(): ByteType(),
        Binary.key(): BinaryType(),
        Integer.key(): IntegerType(),
        Short.key(): ShortType(),
        Long.key(): LongType(),
        Float.key(): FloatType(),
        Double.key(): DoubleType(),
        String.key(): StringType(),
        Date.key(): DateType(),
        DateTime.key(): TimestampType(),
    }
    DF_TYPE_TO_FLYPIPE_TYPE_MAP = {
        "boolean": Boolean(),
        "byte": Byte(),
        "tinyint": Byte(),
        "binary": Binary(),
        "int": Integer(),
        "integer": Integer(),
        "short": Short(),
        "smallint": Short(),
        "long": Long(),
        "bigint": Long(),
        "float": Float(),
        "double": Double(),
        "string": String(),
        "timestamp": DateTime(),
        "date": Date(),
    }

    def _select_columns(self, columns):

        df_cols = self.df.columns

        if not set(columns).issubset(set(df_cols)):
            raise DataFrameMissingColumns(df_cols, columns)

        return self.df.select(list(columns))

    def get_column_flypipe_type(self, target_column):
        return self._get_column_flypipe_type(self.df, target_column)

    def _get_column_flypipe_type(self, df, target_column):
        try:
            dtype = df.schema[target_column].dataType
        except KeyError as exc:
            raise ValueError(
                f'Column "{target_column}" not found in df, available columns are {df.columns}'
            ) from exc
        if isinstance(dtype, DecimalType):
            flypipe_type = Decimal(precision=dtype.precision, scale=dtype.scale)
        else:
            try:
                flypipe_type = self.DF_TYPE_TO_FLYPIPE_TYPE_MAP[dtype.typeName()]
            except KeyError:
                flypipe_type = Unknown()
        return flypipe_type

    def _cast_column(self, column: str, flypipe_type: Type, df_type):
        col = self.df[column]
        result = self._try_cast_col(col, column, df_type)
        self.df = self.df.withColumn(column, result)

    def _try_cast_col(self, col, column: str, df_type):
        """Use try_cast (null on error). Uses Column.try_cast on Spark 4+, F.expr on Spark 3.x."""
        try_cast_method = getattr(col, "try_cast", None)
        if callable(try_cast_method):
            return try_cast_method(df_type)
        type_str = df_type.simpleString()
        return F.expr(f"try_cast(`{column}` AS {type_str})")

    def _cast_column_decimal(self, column, flypipe_type):
        df_type = DecimalType(
            precision=flypipe_type.precision, scale=flypipe_type.scale
        )
        col = self.df[column]
        result = self._try_cast_col(col, column, df_type)
        self.df = self.df.withColumn(column, result)

    def _cast_column_date(self, column, flypipe_type):
        date_col = F.col(column)
        fmt = flypipe_type.pyspark_format
        if hasattr(F, "try_to_date"):
            result = F.try_to_date(date_col, fmt)
        else:
            result = F.to_date(date_col, fmt)
        self.df = self.df.withColumn(column, result)

    def _cast_column_datetime(self, column, flypipe_type):
        ts_col = F.col(column)
        fmt = flypipe_type.pyspark_format
        if hasattr(F, "try_to_timestamp"):
            result = F.try_to_timestamp(ts_col, fmt)
        else:
            result = F.to_timestamp(ts_col, fmt)
        self.df = self.df.withColumn(column, result)
