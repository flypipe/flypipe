import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from flypipe.dataframe_wrapper import DataframeWrapper
from flypipe.node_graph import NodeGraph, RunStatus
from collections import namedtuple
from types import FunctionType

from flypipe.utils import DataFrameType

logger = logging.getLogger(__name__)


class Transformation:

    TYPE_MAP = {
        'pyspark': DataFrameType.PYSPARK,
        'pandas': DataFrameType.PANDAS,
        'pandas_on_spark': DataFrameType.PANDAS_ON_SPARK,
    }

    def __init__(self, function, type: str, dependencies=None, output=None, spark_context=False):
        self.function = function
        self.dependencies = dependencies or []
        try:
            self.type = self.TYPE_MAP[type]
        except KeyError:
            raise ValueError(f'Invalid type {type}, expected one of {",".join(self.TYPE_MAP.keys())}')
        self._provided_inputs = {}
        self.output_schema = output
        self.node_graph = NodeGraph(self)
        self.spark_context = spark_context

    @property
    def __name__(self):
        """Return the name of the wrapped transformation rather than the name of the decorator object"""
        return self.function.__name__

    @property
    def __doc__(self):
        """Return the docstring of the wrapped transformation rather than the docstring of the decorator object"""
        return self.function.__doc__

    def inputs(self, **kwargs):
        for k, v in kwargs.items():
            self._provided_inputs[k] = v

        return self

    def clear_inputs(self):
        self._provided_inputs = {}

    def __call__(self, *args):
        return self.function(*args)

    def run(self, spark=None, parallel=True):
        self.node_graph.calculate_graph_run_status(self.__name__, self._provided_inputs)
        if parallel:
            return self._run_parallel(spark)
        else:
            return self._run_sequential(spark)

    def _run_sequential(self, spark=None):
        outputs = {k: DataframeWrapper(spark, v, schema=None) for k, v in self._provided_inputs.items()}
        node_graph = self.node_graph.copy()
        while not node_graph.is_empty():
            nodes = node_graph.pop_runnable_nodes()
            for node in nodes:
                if node['name'] in outputs:
                    continue

                node_dependencies = {}
                for input_transformation in node['transformation'].dependencies:
                    node_dependencies[input_transformation.__name__] = outputs[input_transformation.__name__].as_type(node['transformation'].type)

                result = self.process_transformation(spark, node['transformation'], **node_dependencies)

                outputs[node['name']] = DataframeWrapper(spark, result, node['transformation'].output_schema)
        return outputs[node['name']].as_type(self.type)

    def _run_parallel(self, node_graph, spark=None):
        # TODO- fix this to run with the new style, see _run_sequential for the correct way of doing things
        outputs = {}
        dependency_map = node_graph.get_dependency_map()
        result_futures = []

        def process_and_cache_node(node_obj, **inputs):
            result = self.process_node(node_obj, **inputs)
            try:
                outputs[node_obj.__name__] = self.validate_dataframe(
                    node_obj.output_schema, result
                )
                for dependency in dependency_map.values():
                    try:
                        dependency.remove(node_obj.__name__)
                    except KeyError:
                        pass
                logger.debug(f'Finished processing node {node_obj.__name__}')
            except TypeError as ex:
                raise TypeError(
                    f"Validation failure on node {node_obj.__name__} when checking output schema: \n{str(ex)}"
                )

        def run_valid_nodes():
            """Run nodes with no yet-to-be run dependencies/inputs"""
            remaining_nodes = list(dependency_map.keys())
            for node_name in remaining_nodes:
                dependencies = dependency_map[node_name]
                if not dependencies:
                    logger.debug(f"Started processing node {node_name}")

                    node_obj = node_graph.get_node_transformation(node_name)
                    try:
                        node_inputs = {}
                        for input_node, input_schema in node_obj.inputs:
                            # We need to ensure that the result of each input is converted to the same type as the node
                            # that's processing it.
                            input_value = input_node.convert_dataframe(
                                outputs[input_node.__name__], node_obj.TYPE
                            )
                            if input_schema:
                                try:
                                    input_value = self.validate_dataframe(
                                        input_schema, input_value
                                    )
                                except TypeError as ex:
                                    raise TypeError(
                                        f"Validation failure on node {node_name} when checking input node "
                                        f"{input_node.__name__}: \n{str(ex)}"
                                    )
                            node_inputs[input_node.__name__] = input_value
                    except KeyError as ex:
                        raise ValueError(
                            f"Unable to process transformation {node_obj.__name__}, missing input {ex.args[0]}"
                        )

                    logger.debug(f"Processing node {node_obj.__name__}")
                    new_task = executor.submit(process_and_cache_node, node_obj, **node_inputs)
                    # Remove the node from the dependency map so that we don't run it again
                    dependency_map.pop(node_obj.__name__)
                    result_futures.append(
                        new_task
                    )

        with ThreadPoolExecutor() as executor:
            run_valid_nodes()
            # As each node is completed we check if it's possible to run any new nodes
            for task in as_completed(result_futures):
                # This is not actually a no-op- if any exceptions occur during node processing they get re-raised when
                # calling result()
                task.result()
                run_valid_nodes()
        return outputs[self.function.__name__]

    # TODO- move this method elsewhere
    @classmethod
    def validate_dataframe(cls, schema, df):
        """
        Ensure the dataframe matches the given schema. There are a few actions here:
        - If the schema requests a column which the dataframe doesn't have then throw an error.
        - If the types of any columns in the dataframe don't match (and aren't castable from) the schema type then throw an error.
        - If the dataframe contains any extra columns which the schema didn't request then filter those columns out.

        The idea here is that we either don't have any schema requirements at all on a transformation (for brevity
        perhaps) in which case this function doesn't get called at all. Otherwise if we are providing a schema then we
        will be very strict with the check.
        """
        selected_columns = []
        df_types = cls.get_column_types(df)
        errors = []
        if schema is not None:
            for column in schema.columns:
                if column.name not in df_types:
                    errors.append(f'Column "{column}" missing from dataframe')
                    continue
                selected_columns.append(column.name)
                flypipe_type = column.type
                try:
                    flypipe_type.validate(df, column.name)
                except TypeError as ex:
                    errors.append(str(ex))
        else:
            selected_columns = df.columns.to_list()
        if errors:
            raise TypeError("\n".join([f"- {error}" for error in errors]))
        # Restrict dataframe to the columns that we requested in the schema
        return df[selected_columns]

    def process_transformation(self, spark, transformation, **inputs):
        # TODO: apply output validation + rename function to transformation
        if transformation.spark_context:
            parameters = {'spark': spark, **inputs}
        else:
            parameters = inputs
        return transformation.function(**parameters)

    def plot(self):
        self.node_graph.plot()



def node(*args, **kwargs):
    """
    Decorator factory that returns the given function wrapped inside a Node class
    """

    def decorator(func):
        return Transformation(func, *args, **kwargs)

    return decorator
