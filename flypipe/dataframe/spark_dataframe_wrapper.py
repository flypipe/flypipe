import pyspark.sql.functions as F
from pyspark.sql.types import BooleanType, ByteType, BinaryType, IntegerType, ShortType, LongType, FloatType, \
    DoubleType, StringType, DecimalType
from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.schema.types import Type, Boolean, Byte, Binary, Integer, Short, Long, Float, Double, String, Unknown
from flypipe.exceptions import DataFrameMissingColumns
from flypipe.utils import DataFrameType


class SparkDataFrameWrapper(DataFrameWrapper):

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
    }
    DF_TYPE_TO_FLYPIPE_TYPE_MAP = {
        'boolean': Boolean(),
        'byte': Byte(),
        'tinyint': Byte(),
        'binary': Binary(),
        'int': Integer(),
        'integer': Integer(),
        'short': Short(),
        'smallint': Short(),
        'long': Long(),
        'bigint': Long(),
        'float': Float(),
        'double': Double(),
        'string': String(),
    }

    def _select_columns(self, columns):

        df_cols = [col for col, _ in self.df.dtypes]

        if not set(columns).issubset(set(df_cols)):
            raise DataFrameMissingColumns(df_cols, columns)

        return self.df.select(list(columns))

    def get_column_flypipe_type(self, target_column):
        for column, dtype in self.df.dtypes:
            if column == target_column:
                try:
                    flypipe_type = self.DF_TYPE_TO_FLYPIPE_TYPE_MAP[dtype]
                except KeyError:
                    flypipe_type = Unknown()
                return flypipe_type
        raise ValueError(f'Column "{target_column}" not found in df, available columns are {self.df.columns}')

    def _cast_column(self, column: str, flypipe_type: Type, df_type):
        self.df = self.df.withColumn(column, self.df[column].cast(df_type))

    def _cast_column_decimal(self, column, flypipe_type):
        df_type = DecimalType(precision=flypipe_type.precision, scale=flypipe_type.scale)
        self.df = self.df.withColumn(column, self.df[column].cast(df_type))

    def _cast_column_date(self, column, flypipe_type):
        self.df = self.df.withColumn(column, F.to_date(F.col(column), flypipe_type.fmt))

    def _cast_column_datetime(self, column, flypipe_type):
        self.df = self.df.withColumn(column, F.to_date(F.col(column), flypipe_type.fmt))
