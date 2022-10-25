from flypipe.node import Node
from flypipe.node_run_context import NodeRunContext
from flypipe.utils import DataFrameType


class PandasOnSparkNode(Node):

    @property
    def input_dataframe_type(self):
        context = NodeRunContext.get_instance()
        if context.get_value('pandas_on_spark_use_pandas'):
            return DataFrameType.PANDAS
        else:
            return DataFrameType.PANDAS_ON_SPARK
