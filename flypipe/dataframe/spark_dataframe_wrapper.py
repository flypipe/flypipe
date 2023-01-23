import pyspark.sql.functions as F
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    BinaryType,
    IntegerType,
    ShortType,
    LongType,
    FloatType,
    DoubleType,
    StringType,
    DecimalType,
    DateType,
    TimestampType,
)

from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.exceptions import DataFrameMissingColumns
from flypipe.schema.types import (
    Type,
    Boolean,
    Byte,
    Binary,
    Integer,
    Short,
    Long,
    Float,
    Double,
    String,
    Unknown,
    Date,
    DateTime,
    Decimal,
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

        df_cols = [col for col, _ in self.df.dtypes]

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
        self.df = self.df.withColumn(column, self.df[column].cast(df_type))

    def _cast_column_decimal(self, column, flypipe_type):
        df_type = DecimalType(
            precision=flypipe_type.precision, scale=flypipe_type.scale
        )
        self.df = self.df.withColumn(column, self.df[column].cast(df_type))

    def _cast_column_date(self, column, flypipe_type):
        self.df = self.df.withColumn(
            column, F.to_date(F.col(column), flypipe_type.pyspark_format)
        )

    def _cast_column_datetime(self, column, flypipe_type):
        self.df = self.df.withColumn(
            column, F.to_date(F.col(column), flypipe_type.pyspark_format)
        )
