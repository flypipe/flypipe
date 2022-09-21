from flypipe.node import Node
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, DateType
from flypipe.schema.type import SchemaType


class SparkNode(Node):

    TYPE = 'spark'
    TYPE_MAP = {
        SchemaType.STRING: StringType,
        SchemaType.INTEGER: IntegerType,
        SchemaType.DECIMAL: DecimalType,
        SchemaType.BOOLEAN: BooleanType,
        SchemaType.DATE: DateType,
        SchemaType.DATETIME: DateType,
    }

    @classmethod
    def convert_dataframe(cls, df, destination_type):
        if destination_type == 'spark':
            return df
        elif destination_type == 'pandas':
            return df.toPandas()
        else:
            raise ValueError(f'No mapping defined to convert a spark node result to type {destination_type}')

    @classmethod
    def get_column_types(cls, df):
        return df.dtypes.to_dict()
