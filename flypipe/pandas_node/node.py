import numpy as np
from flypipe.node import Node
from flypipe.schema.type import SchemaType


class PandasNode(Node):

    TYPE = 'pandas'
    TYPE_MAP = {
        SchemaType.STRING: np.object,
        SchemaType.INTEGER: np.int64,
        SchemaType.DECIMAL: np.float,
        SchemaType.BOOLEAN: np.bool,
        SchemaType.DATE: np.object,
        SchemaType.DATETIME: np.object,
    }

    @classmethod
    def convert_dataframe(cls, df, destination_type):
        if destination_type == 'pandas':
            return df
        elif destination_type == 'spark':
            # Local import as we won't necessarily have spark installed
            from flypipe.spark_node.context import get_spark_session
            spark = get_spark_session()
            return spark.createDataFrame(df)
        else:
            raise ValueError(f'No mapping defined to convert a pandas node result to type "{destination_type}"')

    @classmethod
    def validate_dataframe(cls, schema, df):
        selected_columns = []
        df_pandas_types = df.dtypes.to_dict()
        errors = []
        if schema is not None:
            for column in schema.columns:
                if column.name not in df_pandas_types:
                    errors.append(f'Column "{column}" missing from dataframe')
                    continue
                selected_columns.append(column.name)
                pandas_type = df_pandas_types[column.name]
                flypipe_type = column.type
                if cls.TYPE_MAP[flypipe_type] != pandas_type:
                    errors.append(
                        f'Column {column.name} is of pandas type "{pandas_type}" but we are expecting type '
                        f'"{cls.TYPE_MAP[flypipe_type]}"')
        else:
            selected_columns = df.columns.to_list()
        if errors:
            raise TypeError('\n'.join([f'- {error}' for error in errors]))
        # Restrict dataframe to the columns that we requested in the schema
        return df[selected_columns]
