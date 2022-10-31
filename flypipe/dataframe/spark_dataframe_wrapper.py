import pyspark.sql.functions as F
from pyspark.sql.types import BooleanType, ByteType, BinaryType, IntegerType, ShortType, LongType, FloatType, \
    DoubleType, StringType, DecimalType
from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.schema.types import Type, Boolean, Byte, Binary, Integer, Short, Long, Float, Double, String
from flypipe.utils import DataFrameType


class SparkDataFrameWrapper(DataFrameWrapper):

    DF_TYPE = DataFrameType.PYSPARK
    _TYPE_MAP = {
        Boolean.key(): BooleanType(),
        Byte.key(): ByteType(),
        Binary.key(): BinaryType(),
        Integer.key(): IntegerType(),
        Short.key(): ShortType(),
        Long.key(): LongType(),
        Float.key(): FloatType(),
        Double.key(): DoubleType(),
        String.key(): StringType(),
    }

    def _select_columns(self, columns):
        return self.df.select(list(columns))

    def _cast_column(self, column: str, flypipe_type: Type, df_type):
        self.df = self.df.withColumn(column, self.df[column].cast(df_type))

    def _cast_column_decimal(self, column, flypipe_type):
        df_type = DecimalType(precision=flypipe_type.precision, scale=flypipe_type.scale)
        self.df = self.df.withColumn(column, self.df[column].cast(df_type))

    def _cast_column_date(self, column, flypipe_type):
        self.df = self.df.withColumn(column, F.to_date(F.col(column), flypipe_type.fmt))

    def _cast_column_datetime(self, column, flypipe_type):
        self.df = self.df.withColumn(column, F.to_date(F.col(column), flypipe_type.fmt))
