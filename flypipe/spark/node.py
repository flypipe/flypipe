from flypipe.node import Node


class SparkNode(Node):

    TYPE = 'spark'

    @classmethod
    def convert_dataframe(cls, df, destination_type):
        if destination_type == 'spark':
            return df
        elif destination_type == 'pandas':
            return df.toPandas()
        else:
            raise ValueError(f'No mapping defined to convert a spark node result to type {destination_type}')
