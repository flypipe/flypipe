from flypipe.converter.dataframe import DataFrameConverter
from flypipe.converter.schema import SchemaConverter
from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.utils import DataFrameType


class NodeResult:

    def __init__(self, spark, df, schema):
        self.spark = spark
        self.df = DataFrameWrapper.get_instance(spark, df, schema)
        self.schema = schema
        # TODO- should we create an instance level cache decorator instead of doing this manually?
        self.cached_conversions = {}
        self.dataframe_converter = DataFrameConverter(spark)

    def select_columns(self, *columns):
        self.df = self.df.select_columns(*columns)
        return self.df

    def as_type(self, df_type: DataFrameType):
        if df_type not in self.cached_conversions:
            self.cached_conversions[df_type] = self._as_type(df_type)
        return self.cached_conversions[df_type]

    def _as_type(self, df_type: DataFrameType):
        if self.df.TYPE == df_type:
            dataframe = self.df
        else:
            # TODO- is this a good idea? We are having to reach into self.df to grab the df, this usually is a mark of a design issue
            dataframe = DataFrameWrapper.get_instance(self.spark, self.dataframe_converter.convert(self.df.df, df_type), self.schema)
            if self.schema:
                dataframe = SchemaConverter.cast(dataframe, self.TYPE, self.schema)
        return dataframe
