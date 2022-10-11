from pyspark.sql.types import DecimalType, ArrayType, MapType, StructType

from flypipe.data_type import data_types, Decimals, Array, Map
from flypipe.schema import Column
from flypipe.schema.schema import Schema
from flypipe.utils import DataFrameType, dataframe_type


class PySparkSchemaReader:
    """Reads a given dataframe and creates its Flypipe Schema"""

    @classmethod
    def _get_spark_types(cls):
        supported_types = {}
        for data_type in data_types:
            supported_types[data_type.spark_type.__class__] = data_type

        return supported_types

    @classmethod
    def read(cls, df):
        assert dataframe_type(df) == DataFrameType.PYSPARK, 'Error: dataframe is not of type pyspark'
        supported_types = cls._get_spark_types()

        return cls._read_pyspark(df, supported_types)

    @classmethod
    def build_map(cls, data_type, supported_types):
        element = data_type
        types_ = []
        while element.__class__ == MapType:
            types_ = [(element.keyType, element.valueType)] + types_
            element = element.valueType

        my_type = None
        for type_ in types_:
            if my_type is None:
                my_type = Map(supported_types[type_[0].__class__](),
                                  supported_types[type_[1].__class__]())
            else:
                my_type = Map(supported_types[type_[0].__class__](),
                                  my_type)

        return my_type

    @classmethod
    def build_array(cls, data_type, supported_types):
        element = data_type
        types_ = []
        while element.__class__ == ArrayType:
            types_ = [element.elementType] + types_
            element = element.elementType


        my_type = None
        for type_ in types_:
            if my_type is None:
                my_type = Array(supported_types[type_.__class__]())
            else:
                my_type = Array(my_type)

        return my_type



    @classmethod
    def _read_pyspark(cls, df, supported_types):
        columns = []
        print("888888888888888")

        print(supported_types.keys())
        for schema_ in df.schema:
            column = schema_.name

            # print(schema_.dataType, schema_.dataType.__class__)
            print(schema_.dataType.__class__.__name__, Array.spark_type.__name__,
                  type(schema_.dataType.__class__.__name__), type(Array.spark_type.__name__))
            spark_data_type = None
            if schema_.dataType.__class__ == Decimals.spark_type:
                spark_data_type = Decimals(precision=schema_.dataType.precision,
                                              scale=schema_.dataType.scale)

            elif schema_.dataType.__class__.__name__ == Array.spark_type.__name__:
                spark_data_type = cls.build_array(schema_.dataType, supported_types)

            elif schema_.dataType.__class__.__name__ == Map.spark_type.__name__:
                spark_data_type = cls.build_map(schema_.dataType, supported_types)

            else:
                spark_data_type = supported_types[schema_.dataType.__class__]
                spark_data_type = spark_data_type()

            comment = schema_.metadata['comment'] if 'comment' in schema_.metadata else "no description"

            columns.append(Column(column, spark_data_type, comment))

        return Schema(columns)

