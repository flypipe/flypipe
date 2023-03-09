import os
import inspect
import json
import logging
import types
from pathlib import Path

from flypipe.config import get_config
from flypipe.node_function import NodeFunction
from flypipe.node_graph import RunStatus
from flypipe.template import get_template


logger = logging.getLogger(__name__)


class CatalogNode:
    """
    Wrapper around a regular node that contains some extra attributes that are important for the catalog.
    """

    def __init__(self, node, node_graph=None):
        self.node = node
        self.node_graph = node_graph
        # TODO: it's a little awkward to deal with this node function logic here, is this even the behaviour we want?
        self.predecessors = []
        self.predecessor_columns = {}
        for input_node in node.input_nodes:
            if isinstance(input_node.node, NodeFunction):
                expanded_node = input_node.node.expand(None)[-1]
                self.predecessors.append(expanded_node.key)
                self.predecessor_columns[expanded_node.key] = (
                    input_node.selected_columns or []
                )
            else:
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
        return {
            "nodeKey": self.node.key,
            "nodeType": self.node.type,
            "name": self.node.__name__,
            "description": self.node.description,
            "tags": [{"id": tag, "text": tag} for tag in self.node.tags],
            "filePath": self._get_file_path(),
            "importCmd": self._get_import_cmd(),
            "output": self._get_schema(),
            "predecessors": self.predecessors,
            "predecessorColumns": self.predecessor_columns,
            "successors": sorted(list(self.successors)),
            "sourceCode": self._get_source_code(),
            "isActive": self._get_is_active(),
        }

    def _get_is_active(self):
        if getattr(self, "node_graph"):
            return self.node_graph.get_node(self.node.key)["status"] != RunStatus.SKIP
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


class Catalog:
    """
    The Flypipe catalog is a UI screen which allows for easy browsing of nodes and creation of new nodes via an
    interactive node builder. The nodes in the catalog need to be manually registered before the catalog is rendered.
    """

    def __init__(self):
        self.nodes = {}
        self.initial_nodes = []

    def register_node(self, node, successor=None, node_graph=None):
        if isinstance(node, NodeFunction):
            expanded_nodes = node.expand(None)
            self.register_node(expanded_nodes[-1], successor, node_graph)
        else:
            if node.node_graph is not None:
                # The node graph gives us certain information about the nodes in the context of a single run, use this
                # if available.
                node_graph = node.node_graph
            if node.key not in self.nodes:
                self.nodes[node.key] = CatalogNode(node, node_graph)
            if successor:
                self.nodes[node.key].register_successor(successor)
            for input_node in node.input_nodes:
                self.register_node(input_node.node, node, node_graph)

    def add_node_to_graph(self, node):
        """
        Ordinarily the catalog graph start out as empty but we can add nodes to it here such that the graph starts with
        them present.
        """
        if isinstance(node, NodeFunction):
            expanded_nodes = node.expand(None)
            self.initial_nodes.append(expanded_nodes[-1].key)
        else:
            self.initial_nodes.append(node.key)

    def html(self, height=850):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_path, "js/bundle.js"), "r", encoding="utf-8") as f:
            js_bundle = f.read()
        return get_template("catalog.html").render(
            js_bundle=js_bundle,
            nodes=json.dumps(self.get_node_defs()),
            initialNodes=self.initial_nodes,
            tagSuggestions=json.dumps(self.get_tag_suggestions()),
            height=height,
        )

    def get_node_defs(self):
        return [node.get_def() for node in self.nodes.values()]

    def get_tag_suggestions(self):
        all_tags = set()
        for catalog_node in self.nodes.values():
            node = catalog_node.node
            all_tags = all_tags.union(set(node.tags))
        return [{"id": tag, "text": tag} for tag in sorted(list(all_tags))]

    def get_count_box_defs(self):
        """
        We want to show a list of counts at the top of the catalog screen showing how many nodes the catalog currently
        has, as well as the node count under certain categorical labels.
        """
        count_box_defs = [{"label": "nodes", "count": len(self.nodes)}]
        raw_config = get_config("catalog_count_box_tags")
        if not raw_config:
            return count_box_defs
        tags = raw_config.split(",")
        tag_count = {tag: 0 for tag in tags}
        for catalog_node in self.nodes.values():
            for tag in catalog_node.node.tags:
                if tag in tag_count:
                    tag_count[tag] += 1
        for tag in tags:
            count_box_defs.append({"label": tag, "count": tag_count[tag]})
        return count_box_defs


if __name__ == "__main__":
    catalog = Catalog()
    # *** Register nodes here ***
    # catalog.register_node(...)
    with open("catalog.html", "w", encoding="utf-8") as f:
        f.write(catalog.html())
