import snowflake.snowpark.functions as F
from snowflake.snowpark.types import (
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


class SnowparkDataFrameWrapper(DataFrameWrapper):
    """
    Wrapper around a Snowpark dataframe. This gives some conversion functionality between Flypipe types and their
    Snowpark equivalents.
    """

    DF_TYPE = DataFrameType.SNOWPARK
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
        "varchar": String(),
        "text": String(),
        "timestamp": DateTime(),
        "timestamp_ntz": DateTime(),
        "timestamp_ltz": DateTime(),
        "timestamp_tz": DateTime(),
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
            dtype = df.schema[target_column].datatype
        except KeyError as exc:
            raise ValueError(
                f'Column "{target_column}" not found in df, available columns are {df.columns}'
            ) from exc
        if isinstance(dtype, DecimalType):
            flypipe_type = Decimal(precision=dtype.precision, scale=dtype.scale)
        else:
            # Get type name from Snowpark datatype
            type_name = dtype.typeName().lower()
            print(f"Column: {target_column}, Type class: {type(dtype).__name__}, typeName: {type_name}, dtype: {dtype.simple_string()}  ")
            try:   
                flypipe_type = self.DF_TYPE_TO_FLYPIPE_TYPE_MAP[type_name]
            except KeyError:
                flypipe_type = Unknown()
        return flypipe_type

    def _cast_column(self, column: str, flypipe_type: Type, df_type):
        self.df = self.df.with_column(column, self.df[column].cast(df_type))

    def _cast_column_decimal(self, column, flypipe_type):
        df_type = DecimalType(
            precision=flypipe_type.precision, scale=flypipe_type.scale
        )
        self.df = self.df.with_column(column, self.df[column].cast(df_type))

    def _cast_column_date(self, column, flypipe_type):
        # Snowpark uses to_date function
        self.df = self.df.with_column(
            column, F.to_date(F.col(column), flypipe_type.pyspark_format)
        )

    def _cast_column_datetime(self, column, flypipe_type):
        # Snowpark uses to_timestamp function
        self.df = self.df.with_column(
            column, F.to_timestamp(F.col(column), flypipe_type.pyspark_format)
        )

