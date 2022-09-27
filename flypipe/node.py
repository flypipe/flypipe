import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from flypipe.node_graph import NodeGraph
from collections import namedtuple
from abc import ABC, abstractmethod
from types import FunctionType

logger = logging.getLogger(__name__)


NodeInput = namedtuple("NodeInput", ["node", "schema"])


class Node(ABC):

    TYPES = {}
    TYPE = None

    def __init__(self, transformation, inputs=None, output=None):
        self.transformation = transformation
        self.inputs = []
        if inputs:
            for input_def in inputs:
                if isinstance(input_def, tuple):
                    node, schema = input_def
                    self.inputs.append(NodeInput(node, schema))
                elif isinstance(input_def, Node):
                    self.inputs.append(NodeInput(input_def, None))
                else:
                    raise TypeError(
                        f"Input {input_def} is {type(input_def)} but expected it to be either a tuple or an instance/subinstance of Node"
                    )
        self.output_schema = output
        self.node_graph = NodeGraph(self)

    @classmethod
    def get_class(cls, node_type):
        try:
            return cls.TYPES[node_type]
        except KeyError:
            raise ValueError(
                f"Invalid node type {node_type} specified, provide type must be one of {cls.TYPES.keys()}"
            )

    @classmethod
    def register_node_type(cls, node_type_class):
        cls.TYPES[node_type_class.TYPE] = node_type_class

    @property
    def __name__(self):
        """Return the name of the wrapped transformation rather than the name of the decorator object"""
        return self.transformation.__name__

    @property
    def __doc__(self):
        """Return the docstring of the wrapped transformation rather than the docstring of the decorator object"""
        return self.transformation.__doc__

    def __call__(self, *args, **kwargs):
        return self.transformation(*args, **kwargs)

    @classmethod
    @abstractmethod
    def convert_dataframe(cls, df, destination_type):
        # TODO- is there a better place to put this? It seems a little out of place here
        pass

    @classmethod
    @abstractmethod
    def get_column_types(cls, df):
        pass

    def run(self):
        outputs = {}
        dependency_map = self.node_graph.get_dependency_map()
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

                    node_obj = self.node_graph.get_node(node_name)
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
        return outputs[self.transformation.__name__]

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

    def process_node(self, node_obj, **inputs):
        return node_obj(**inputs)

    def plot(self):
        self.node_graph.plot()


def node(*args, **kwargs):
    """
    Decorator factory that returns the given function wrapped inside a Node class
    """

    def decorator(func):
        try:
            node_type = kwargs.pop("type")
        except KeyError:
            node_type = None
        return Node.get_class(node_type)(func, *args, **kwargs)

    return decorator
