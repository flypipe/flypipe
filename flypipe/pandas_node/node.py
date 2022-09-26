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
    def get_column_types(cls, df):
        return dict(df.dtypes)


