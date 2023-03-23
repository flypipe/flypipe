import os
import json
import logging

from flypipe.catalog.group import Group
from flypipe.catalog.node import CatalogNode
from flypipe.config import get_config
from flypipe.node_function import NodeFunction
from flypipe.template import get_template


logger = logging.getLogger(__name__)


class Catalog:
    """
    The Flypipe catalog is a UI screen which allows for easy browsing of nodes and creation of new nodes via an
    interactive node builder. The nodes in the catalog need to be manually registered before the catalog is rendered.
    """

    def __init__(self):
        self.nodes = {}
        self.groups = {}
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
            if node.group:
                if node.group not in self.groups:
                    self.groups[node.group] = Group(node.group)
                self.groups[node.group].add_node(node)
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
        with open(
            os.path.join(dir_path, "../js/bundle.js"), "r", encoding="utf-8"
        ) as f:
            js_bundle = f.read()
        return get_template("catalog.html").render(
            js_bundle=js_bundle,
            nodes=json.dumps(self.get_nodes()),
            groups=json.dumps(self.get_groups()),
            initialNodes=self.initial_nodes,
            tagSuggestions=json.dumps(self.get_tag_suggestions()),
            height=height,
        )

    def get_nodes(self):
        return [node.get_def() for node in self.nodes.values()]

    def get_groups(self):
        return [group.get_def() for group in self.groups.values()]

    def get_tag_suggestions(self):
        all_tags = set()
        for catalog_node in self.nodes.values():
            node = catalog_node.node
            all_tags = all_tags.union(set(node.tags))
        return [{"id": tag, "name": tag} for tag in sorted(list(all_tags))]

    def get_count_box_defs(self):
        """
        We want to show a list of counts at the top of the catalog screen showing how many nodes the catalog currently
        has, as well as the node count under certain categorical labels.
        """
        # TODO this is currently unused, are we planning to bring it back?
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
