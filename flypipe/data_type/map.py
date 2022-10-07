import warnings

from numpy import dtype
from pyspark.sql.types import MapType
import pyspark.sql.functions as F
from flypipe.data_type.type import Type


class MapContentCast(UserWarning):
    pass


class Map(Type):
    """Casts dataframe to map

    Attributes
    ----------
    key_type: flypipe.data_type.Type
        Defines the type of the keys of the map

    value_type: flypipe.data_type.Type
        Defines the type of the values of the map
    """

    spark_type = None
    pandas_type = dtype("O")

    def __init__(self, key_type, value_type):
        assert (
            key_type is not None and value_type is not None
        ), "Error: please define the type of the Map, ie. Map(Integer(), String())"

        warnings.warn(
            MapContentCast(
                "Make sure the content of the map has been casted to the proper key and value types"
            )
        )


        self.spark_type = MapType(key_type.spark_type, value_type.spark_type)

    def _cast_pyspark(self, df, column: str):
        df = df.withColumn(column, F.col(column).cast(self.spark_type))
        return df

    def _cast_pandas(self, df, column: str):
        df[column] = df[column].astype(self.pandas_type)
        return df

    def _cast_pandas_on_spark(self, df, column: str):
        warnings.warn(
            MapContentCast(
                f"column {column} has not been casted as `astype` can not be applied to maps on pandas on spark, "
                f"retrieving the column as it is"
            )
        )
        return df
