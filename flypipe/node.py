import logging

from flypipe.dataframe_wrapper import DataframeWrapper
from flypipe.node_graph import NodeGraph
from flypipe.node_type import NodeType
from flypipe.printer.graph_html import GraphHTML
from flypipe.transformation import Transformation

logger = logging.getLogger(__name__)


class Node(Transformation):
    node_type = NodeType.TRANSFORMATION

    def _create_graph(self):
        self.node_graph = NodeGraph(self)
        self.node_graph.calculate_graph_run_status(self.__name__, self._provided_inputs)

    def select(self, *columns):
        self.selected_columns = []
        if isinstance(columns[0], list):
            self.selected_columns = list(dict.fromkeys(self.selected_columns + columns[0]))
        else:
            for column in columns:
                self.selected_columns.append(column)
        self.selected_columns = sorted(list(set(self.selected_columns)))
        return self

    def inputs(self, **kwargs):
        for k, v in kwargs.items():
            self._provided_inputs[k.replace(".","_")] = v

        return self

    def clear_inputs(self):
        self._provided_inputs = {}

    def __call__(self, *args):
        return self.function(*args)

    def run(self, spark=None, parallel=True):
        self._create_graph()
        if parallel:
            raise NotImplementedError
        else:
            return self._run_sequential(spark)

    def _run_sequential(self, spark=None):
        outputs = {k: DataframeWrapper(spark, df, schema=None) for k, df in self._provided_inputs.items()}
        node_graph = self.node_graph.copy()

        while not node_graph.is_empty():
            transformations = node_graph.pop_runnable_transformations()
            for transformation in transformations:
                if transformation.__name__ in outputs:
                    continue

                node_dependencies = {}
                for input_transformation in transformation.dependencies:
                    node_dependencies[input_transformation.__name__] = \
                        outputs[input_transformation.__name__].as_type(transformation.type)

                result = self.process_transformation(spark, transformation, **node_dependencies)

                outputs[transformation.__name__] = DataframeWrapper(spark, result, transformation.output_schema)

        return outputs[self.__name__].as_type(self.type)


    def process_transformation(self, spark, transformation: Transformation, **inputs):
        # TODO: apply output validation + rename function to transformation, select only necessary columns specified in self.dependencies_selected_columns
        if transformation.spark_context:
            parameters = {'spark': spark, **inputs}
        else:
            parameters = inputs

        return transformation.function(**parameters)

    def plot(self):
        self.node_graph.plot()

    def html(self, width=-1, height=1000):
        self._create_graph()
        return GraphHTML(self.node_graph, width=width, height=height).get()


def node(*args, **kwargs):
    """
    Decorator factory that returns the given function wrapped inside a Node class
    """

    def decorator(func):
        return Node(func, *args, **kwargs)

    return decorator


class DataSource(Node):
    node_type = NodeType.DATASOURCE


def datasource_node(*args, **kwargs):
    """
    Decorator factory that returns the given function wrapped inside a Datasource Node class
    """

    def decorator(func):
        """TODO: I had to re-create graph in the decorator as selected_columns are set after the node has been created
        when creting a virtual datasource node, it is set the columns manually
        """

        kwargs_init = {k:v for k,v in kwargs.items() if k != 'selected_columns'}
        ds = DataSource(func, *args, **kwargs_init)
        return ds.select(kwargs['selected_columns'])

    return decorator