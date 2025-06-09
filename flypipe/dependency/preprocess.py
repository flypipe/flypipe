import importlib
from typing import Callable, List, Union

from flypipe.config import get_config
from flypipe.dependency.preprocess_mode import PreprocessMode
from flypipe.run_context import RunContext


class Preprocess:
    def __init__(self, mode: PreprocessMode = None, preprocess: [Callable] = None):
        self._preprocess = preprocess or []
        self.preprocess_mode = mode or PreprocessMode.ACTIVE

    def set(self, *function: Union[PreprocessMode, Callable]):

        if not function:
            raise ValueError("Preprocess function must not be empty")

        if isinstance(function[0], PreprocessMode):
            self.preprocess_mode = function[0]

        else:
            self._preprocess = (
                function[0] if isinstance(function[0], list) else list(function)
            )

            for func in self._preprocess:
                if not isinstance(func, Callable):
                    raise ValueError(
                        f"Only callable function are allowed for preprocessing, type {type(func)} not allowed, {self._preprocess}"
                    )
        return self

    def has_preprocess(self):
        return bool(self.preprocess_functions)

    @property
    def preprocess_functions(self) -> List[Callable]:
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

    def apply(
        self,
        run_context: RunContext,
        parent_node: "Node",  # noqa: F821
        dependency_node: "Node",  # noqa: F821
        df,
    ):
        run_process_mode = run_context.get_run_preprocess_mode()
        if run_process_mode.value == PreprocessMode.ACTIVE.value:

            input_node_preprocess_mode_run_context = (
                run_context.get_dependency_preprocess_mode(parent_node, dependency_node)
            )
            if (
                input_node_preprocess_mode_run_context.value
                == PreprocessMode.ACTIVE.value
            ):

                if self.preprocess_mode.value == PreprocessMode.ACTIVE.value:

                    if self.has_preprocess():
                        for func in self.preprocess_functions:
                            df = df.apply(func)

        return df

    def copy(self):
        return Preprocess(self.preprocess_mode, self._preprocess)
