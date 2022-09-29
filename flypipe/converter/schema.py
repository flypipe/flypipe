from flypipe.schema.schema import Schema


class SchemaConverter:
    """Casts a dataframe using a given schema"""

    @staticmethod
    def cast(df, schema: Schema):
        """Casts a dataframe using a given schema

        Parameters
        ----------

        df: pandas, pandas_on_spark or spark dataframe
            dataframe to be casted
        schema: Schema
            the schema definition in which the columns of the dataframe will be casted to the defined data types

        Returns
        -------
        df: pandas, pandas_on_spark or spark dataframe
            dataframe casted to the given schema
        """

        for column in schema.columns:
            df = column.type.cast(df, column.name)
        return df



