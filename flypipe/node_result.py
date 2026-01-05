from __future__ import annotations

from typing import Union, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from snowflake.snowpark.session import Session as SnowflakeSession

from flypipe.converter.dataframe import DataFrameConverter
from flypipe.dataframe.dataframe_wrapper import DataFrameWrapper
from flypipe.utils import DataFrameType


class NodeResult:
    """
    Wrapper around the raw result from a node, allowing for the result to be converted into various format and have
    type casting applied.
    """

    def __init__(self, session: Union["SnowflakeSession", "SparkSession"], df, schema):
        self.session = session
        self.df_wrapper = DataFrameWrapper.get_instance(session, df)
        self._apply_schema_to_df(schema)
        # TODO- should we create an instance level cache decorator instead of doing this manually?
        self.cached_conversions = {}
        self.dataframe_converter = DataFrameConverter(session)

    def _apply_schema_to_df(self, schema):
        """
        The raw dataframe that a node returns might be markedly different from the schema, we need to apply the schema
        to bring the dataframe into line.
        """
        if schema:
            self.df_wrapper = self.df_wrapper.select_columns(
                [column.name for column in schema.columns]
            )
            for column in schema.columns:
                self.df_wrapper.cast_column(column.name, column.type)

    def select_columns(self, *columns):
        return self.df_wrapper.select_columns(*columns)

    def copy(self):
        return self.df_wrapper.copy()

    def as_type(self, df_type: DataFrameType):
        if df_type not in self.cached_conversions:
            self.cached_conversions[df_type] = self._as_type(df_type)
        return self.cached_conversions[df_type]

    def _as_type(self, df_type: DataFrameType):
        if self.df_wrapper.DF_TYPE == df_type:
            dataframe = self.df_wrapper
        else:
            # TODO- is this a good idea? We are having to reach into self.df_wrapper to grab the df, this usually is a
            # mark of a design issue
            dataframe = DataFrameWrapper.get_instance(
                self.session,
                self.dataframe_converter.convert(self.df_wrapper.df, df_type),
            )
        return dataframe
