import importlib
from argparse import ArgumentTypeError
from typing import Callable, List

from flypipe.config import get_config
from flypipe.dependency import PreProcessMode


class InputNode:
    """
    An input node is just a wrapper around a regular node with some extra functionalities on top to allow for usage as
    an input into another node:
    - selected_columns that the receiving node is using.
    - aliasing of the name of the dataframe passed to the receiving node, otherwise a sanitised version of the node
    name is used.
    """

    def __init__(self, node):
        self.node = node
        self._selected_columns = None
        self._alias = None
        self._preprocess = []
        self._preprocess_mode = PreProcessMode.ACTIVE

    @property
    def __name__(self):
        return self.node.__name__

    @property
    def key(self):
        return self.node.key

    @property
    def preprocess_mode(self):
        return self._preprocess_mode

    def set_preprocess_mode(self, preprocess_mode: PreProcessMode):
        self._preprocess_mode = preprocess_mode
        return self

    def preprocess(self, *function: Callable):

        self._preprocess = (
            function[0] if isinstance(function[0], list) else list(function)
        )

        for func in self._preprocess:
            if not isinstance(func, Callable):
                raise ArgumentTypeError(
                    f"Only callable function are allowed for preprocessing, type {type(func)} not allowed, {self._preprocess}"
                )

        return self

    def has_preprocess(self):
        preprocesses = self.node_input_preprocess
        return (
            True
            if preprocesses is not None and len(self.node_input_preprocess) > 0
            else False
        )

    @property
    def node_input_preprocess(self) -> List[Callable]:
        """
        Returns the preprocessing function that will be applied to the node
        if it is not specified, it will use the function defined in the config
        default_dependencies_preprocess_module and default_dependencies_preprocess_function,
        """
        if self._preprocess:
            return self._preprocess
        else:
            module_preprocess = get_config("default_dependencies_preprocess_module")
            function_preprocess = get_config("default_dependencies_preprocess_function")

            try:
                if module_preprocess is not None and function_preprocess is not None:

                    # Import the module
                    module = importlib.import_module(module_preprocess)

                    # Get the function from the module
                    func = getattr(module, function_preprocess)

                    if module is not None and func is not None:
                        return [func]

            except (ModuleNotFoundError, AttributeError):
                error_msg = (
                    f"WARNING: Could not import preprocess function as defined in flypipe config "
                    f"default_dependencies_preprocess_module={module_preprocess} and "
                    f"default_dependencies_preprocess_function={function_preprocess}"
                )
                print(error_msg)

    @property
    def selected_columns(self):
        return self._selected_columns

    def select(self, *columns):
        # TODO- if self.output_schema is defined then we should ensure each of the columns is in it.
        # otherwise if self.output_schema is not defined then we won't know the ultimate output schema
        # so can't do any validation

        cols = columns[0] if isinstance(columns[0], list) else list(columns)

        if len(cols) != len(set(cols)):
            raise ValueError(f"Duplicated columns in selection of {self.__name__}")
        self._selected_columns = sorted(cols)
        return self

    def alias(self, value):
        self._alias = value
        return self

    def get_alias(self):
        if self._alias:
            return self._alias
        # Sometimes the node name will have periods in it, for example if it's coming from a spark table datasource,
        # periods are not valid argument names so let's replace them with underscores.
        return self.__name__.replace(".", "_").replace("<", "").replace(">", "")

    def copy(self):
        # It's necessary to access protected fields to do a deep copy
        input_node_copy = InputNode(self.node.copy())
        input_node_copy._selected_columns = self._selected_columns
        input_node_copy._alias = self._alias
        input_node_copy._preprocess = self._preprocess
        input_node_copy._preprocess_mode = self._preprocess_mode
        return input_node_copy
