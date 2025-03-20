import inspect
import os
import types
from pathlib import Path

from flypipe.run_status import RunStatus


class CatalogNode:
    """
    Wrapper around a regular node that contains some extra attributes that are important for the catalog.
    """

    def __init__(self, node, node_graph=None):
        self.node = node
        self.node_graph = node_graph
        self.predecessors = []
        self.predecessor_columns = {}

        for input_node in node.input_nodes:
            self.predecessors.append(input_node.node.key)
            self.predecessor_columns[input_node.node.key] = (
                input_node.selected_columns or []
            )
        self.successors = set()

    def register_successor(self, successor_node):
        """
        From a node definition we can get the node's predecessors via the dependencies list, however nodes are not in
        themselves aware of their successors. To support a list of successors we need to manually register the
        successor nodes whilst parsing a node graph.
        """
        self.successors.add(successor_node.key)

    def get_def(self):
        tags_set = set()
        for tag in self.node.tags:
            tags_set.add(tag.lower().strip())
        node_run_context = self.node_graph.get_node(self.node.key)["node_run_context"]
        return {
            "nodeKey": self.node.key,
            "nodeType": self.node.type,
            "name": self.node.__name__,
            "description": self.node.description,
            "tags": [{"id": tag, "name": tag} for tag in sorted(list(tags_set))],
            "filePath": self._get_file_path(),
            "importCmd": self._get_import_cmd(),
            "output": self._get_schema(),
            "predecessors": self.predecessors,
            "predecessorColumns": self.predecessor_columns,
            "successors": sorted(list(self.successors)),
            "sourceCode": self._get_source_code(),
            "isActive": self._get_is_active(),
            "hasCache": self.node.cache is not None,
            "cacheIsDisabled": node_run_context.cache_context.disabled
            or node_run_context.exists_provided_input,
            "hasProvidedInput": node_run_context.exists_provided_input,
            "group": self.node.group,
        }

    def _get_is_active(self):
        if getattr(self, "node_graph"):
            return self.node_graph.get_node(self.node.key)["status"] == RunStatus.ACTIVE
        return True

    def _get_file_path(self):
        """
        Get the path of the file holding the node. The inspect.getfile utility function returns an absolute path but we
        would like a path relative to the project root, this is especially important in _get_import_cmd as we use the
        path to generate the import command.

        The method used to calculate the path relative to the project root is simply to iterate over the parent
        directories one by one until we reach a directory that doesn't contain an __init__.py file.
        """
        if self._is_in_notebook():
            return ""
        absolute_path = inspect.getfile(self.node.function)
        base = Path(absolute_path).parent
        while (base / "__init__.py").is_file():
            base = base.parent
        return os.path.relpath(absolute_path, start=base)

    def _get_import_cmd(self):
        if self._is_in_notebook():
            # We can't import functions from notebooks directly
            return ""
        # Remove the .py extension and convert the directory separator / into full stops
        module_path = self._get_file_path()[:-3].replace("/", ".").replace("\\", ".")
        return f"from {module_path} import {self.node.__name__}"

    def _get_schema(self):
        if self.node.output_schema:
            return [
                {
                    "column": column.name,
                    "type": column.type.name,
                    "description": column.description,
                }
                for column in self.node.output_schema.columns
            ]
        return []

    def _get_source_code(self):
        """
        Get the source code of the node. We try in the first instance to load the entire module which is holding the
        node, if the function is defined inside a notebook we can't do this and just load the source code for the node
        function.
        """
        if self._is_in_notebook():
            return inspect.getsource(self.node.function)
        return inspect.getsource(inspect.getmodule(self.node.function))

    def _is_in_notebook(self):
        """Determine if the node is defined inside a notebook."""
        code_module = inspect.getmodule(self.node.function)
        return (
            not isinstance(code_module, types.ModuleType)
        ) or code_module.__name__ == "__main__"
