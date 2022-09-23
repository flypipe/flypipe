import warnings

from pyspark.sql.types import MapType

from flypipe.data_type.type import Type


class MapContentCast(UserWarning):
    pass


class Map(Type):
    """Casts dataframe to array

    Attributes
    ----------
    key_type: flypipe.data_type.Type
        Defines the type of the keys of the map

    value_type: flypipe.data_type.Type
        Defines the type of the values of the map
    """

    spark_data_type = MapType
    pandas_type = dict

    def __init__(self, key_type, value_type):
        assert (
            key_type is not None and value_type is not None
        ), "Error: please define the type of the Map, ie. Map(Integer(), String())"

        warnings.warn(
            MapContentCast(
                "Make sure the content of the map has been casted to the proper key and value types"
            )
        )

        key_pandas_type, key_spark_type = key_type.pandas_type, key_type.spark_type
        value_pandas_type, value_spark_type = (
            value_type.pandas_type,
            value_type.spark_type,
        )

        self.spark_type = MapType(key_spark_type, value_spark_type)
