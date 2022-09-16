from flypipe.node import Node


class PandasNode(Node):

    TYPE = 'pandas'

    @classmethod
    def convert_dataframe(cls, df, destination_type):
        if destination_type == 'pandas':
            return df
        elif destination_type == 'spark':
            # Local import as we won't necessarily have spark installed
            from flypipe.spark.context import get_spark_session
            spark = get_spark_session()
            return spark.createDataFrame(df)
        else:
            raise ValueError(f'No mapping defined to convert a pandas node result to type {destination_type}')
