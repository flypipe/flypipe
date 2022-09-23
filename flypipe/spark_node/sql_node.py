from flypipe.spark_node.node import SparkNode
from flypipe.spark_node.context import get_spark_session


class SparkSQLNode(SparkNode):

    def process_node(self, **inputs):
        spark = get_spark_session()
        new_inputs = {}
        # Convert all input dataframes into temporary views
        for key, input_df in inputs.items():
            view_name = f'{self.__name__}.key'
            input_df.createOrReplaceTempView(view_name)
            new_inputs[key] = view_name
        spark.sql(self.transformation(**new_inputs))
