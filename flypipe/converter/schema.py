from flypipe.schema.schema import Schema


class SchemaConverter:

    @staticmethod
    def cast(df, schema: Schema):
        for column in schema.columns:
            df = column.type.cast(df, column.name)
        return df



