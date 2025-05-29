from flypipe.dataframe.spark_dataframe_wrapper import SparkDataFrameWrapper
from flypipe.utils import DataFrameType

from flypipe.schema.types import (
    Boolean,
    Byte,
    Binary,
    Integer,
    Short,
    Long,
    Float,
    Double,
    String,
    Date,
    DateTime,
)

from sparkleframe.polarsdf.types import (
    BooleanType,
    ByteType,
    BinaryType,
    IntegerType,
    ShortType,
    LongType,
    FloatType,
    DoubleType,
    StringType,
    DateType,
    TimestampType,
)


class SparkleDataFrameWrapper(SparkDataFrameWrapper):
    """
    Wrapper around a Spark dataframe. This gives some conversion functionality between Flypipe types and their pyspark
    equivalents.
    """

    DF_TYPE = DataFrameType.SPARKLEFRAME

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

    # def get_df(self):
    #     print(type(self.df))
    #     return PolarsDataFrame(self.df)
