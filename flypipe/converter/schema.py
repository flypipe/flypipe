from flypipe.schema.schema import Schema
from flypipe.utils import DataFrameType


# TODO- do we need this
# class SchemaConverter:
#     """Casts a dataframe using a given schema"""
#
#     @staticmethod
#     def cast(df, df_type: DataFrameType, schema: Schema):
#         """Casts a dataframe using a given schema
#
#         Parameters
#         ----------
#
#         df: pandas, pandas_on_spark or spark dataframe
#             dataframe to be casted
#         df_type: DataFrameType
#             type of the dataframe
#         schema: Schema
#             the schema definition in which the columns of the dataframe will be casted to the defined data types
#
#         Returns
#         -------
#         df: pandas, pandas_on_spark or spark dataframe
#             dataframe casted to the given schema
#
#         """
#
#         for column in schema.columns:
#             df = column.type.cast(df, df_type, column.name)
#         return df
#
#
#
