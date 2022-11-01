from flypipe.converter.dataframe import DataFrameConverter
from flypipe.converter.schema import SchemaConverter
from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.utils import DataFrameType


class NodeResult:

    def __init__(self, spark, raw_df, schema):
        self.spark = spark
        raw_df_wrapper = DataFrameWrapper.get_instance(spark, raw_df)
        self.df = self._apply_schema_to_df(spark, raw_df_wrapper, schema)
        # TODO- should we create an instance level cache decorator instead of doing this manually?
        self.cached_conversions = {}
        self.dataframe_converter = DataFrameConverter(spark)

    @classmethod
    def _apply_schema_to_df(cls, spark, df_wrapper, schema):
        """
        The raw dataframe that a node returns might be markedly different from the schema, we need to apply the schema
        to bring the dataframe into line.
        """
        # TODO- implement check to error out if there's a column in the schema which isn't in the output
        # TODO- implement code to select only columns that are in the schema
        for column in schema.columns:
            df_wrapper.cast_column(column.name, column.type)
        return None

    def select_columns(self, *columns):
        self.df = self.df.select_columns(*columns)
        return self.df

    def as_type(self, df_type: DataFrameType):
        if df_type not in self.cached_conversions:
            self.cached_conversions[df_type] = self._as_type(df_type)
        return self.cached_conversions[df_type]

    def _as_type(self, df_type: DataFrameType):
        if self.df.DF_TYPE == df_type:
            dataframe = self.df
        else:
            # TODO- is this a good idea? We are having to reach into self.df to grab the df, this usually is a mark of a design issue
            dataframe = DataFrameWrapper.get_instance(self.spark, self.dataframe_converter.convert(self.df.df, df_type))
        return dataframe
