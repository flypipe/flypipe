import os
import inspect
import json
from pathlib import Path

from flypipe.node_function import NodeFunction
from flypipe.template import get_template


class CatalogNode:
    """
    Wrapper around a regular node that contains some extra attributes that are important for the catalog.
    """

    def __init__(self, node):
        self.node = node
        # TODO: it's a little awkward to deal with this node function logic here, is this even the behaviour we want?
        self.predecessors = [
            input_node.node.__name__
            if not isinstance(input_node.node, NodeFunction)
            else input_node.node.expand(None)[-1].__name__ for input_node in node.input_nodes
        ]
        self.successors = set()

    def register_successor(self, successor_node):
        """
        From a node definition we can get the node's predecessors via the dependencies list, however nodes are not in
        themselves aware of their successors. To support a list of successors we need to manually register the
        successor nodes whilst parsing a node graph.
        """
        self.successors.add(successor_node.__name__)

    def get_def(self):
        return {
            "key": self.node.key,
            "name": self.node.__name__,
            "description": self.node.description,
            "tags": self.node.tags,
            "filePath": self._get_file_path(),
            "importCmd": self._get_import_cmd(),
            "schema": self._get_schema(),
            "predecessors": self.predecessors,
            "successors": sorted(list(self.successors)),
        }

    def _get_file_path(self):
        """
        Get the path of the file holding the node. The inspect.getfile utility function returns an absolute path but we
        would like a path relative to the project root, this is especially important in _get_import_cmd as we use the
        path to generate the import command.

        The method used to calculate the path relative to the project root is simply to iterate over the parent
        directories one by one until we reach a directory that doesn't contain an __init__.py file.
        """
        absolute_path = inspect.getfile(self.node.function)
        base = Path(absolute_path).parent
        while (base / '__init__.py').is_file():
            base = base.parent
        return os.path.relpath(absolute_path, start=base)

    def _get_import_cmd(self):
        # Remove the .py extension and convert the directory separator / into full stops
        module_path = self._get_file_path()[:-3].replace("/", ".").replace("\\", ".")
        return f"from {module_path} import {self.node.__name__}"

    def _get_schema(self):
        if self.node.output_schema:
            return [column.name for column in self.node.output_schema.columns]
        return []


class Catalog:
    """
    The Flypipe catalog is a UI screen which allows for easy browsing of nodes and creation of new nodes via an
    interactive node builder. The nodes in the catalog need to be manually registered before the catalog is rendered.
    """

    def __init__(self):
        self.nodes = {}

    def register_node(self, node, successor=None):
        if isinstance(node, NodeFunction):
            expanded_nodes = node.expand(None)
            self.register_node(expanded_nodes[-1], successor)
        else:
            if node.key not in self.nodes:
                self.nodes[node.key] = CatalogNode(node)
            if successor:
                self.nodes[node.key].register_successor(successor)
            for input_node in node.input_nodes:
                self.register_node(input_node.node, node)

    def html(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_path, "js/bundle.js"), "r", encoding="utf-8") as f:
            js_bundle = f.read()
        return get_template("catalog.html").render(
            js_bundle=js_bundle, nodes=json.dumps(self.get_node_defs())
        )

    def get_node_defs(self):
        return [node.get_def() for node in self.nodes.values()]


if __name__ == "__main__":
    catalog = Catalog()
    # *** Register nodes here ***
    # catalog.register_node(...)
    with open("catalog.html", "w", encoding="utf-8") as f:
        f.write(catalog.html())
