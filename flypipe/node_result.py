from flypipe.converter.schema import SchemaConverter
from flypipe.dataframe.dataframe import DataFrame


class NodeResult:

    def __init__(self, spark, df, schema):
        self.df = DataFrame.get_class(df)(spark, df, schema)
        # TODO- should we create an instance level cache decorator instead of doing this manually?
        self.cached_conversions = {}

    def select_columns(self, *columns):
        self.df = self.df.select_columns(*columns)

    def as_type(self, df_type):
        if df_type not in self.cached_conversions:
            self.cached_conversions[df_type] = self.df.as_type(df_type)
        return self.cached_conversions[df_type]
