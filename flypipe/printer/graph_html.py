import json
import os

from flypipe.node_graph import RunStatus, NodeGraph
from flypipe.node_type import NodeType
from flypipe.printer.template import get_template
from flypipe.schema.types import Unknown
from flypipe.utils import DataFrameType


class GraphHTML:
    """
    Model wrapper over a node graph that is responsible of generating a HTML UI representation of the DAG graph.
    """

    CSS_MAP = {
        "pandas": {
            "shape": "circle",
            "bg-class": "success",
            "bg-color": "#198754",
            "text": DataFrameType.PANDAS.value,
        },
        "pyspark": {
            "shape": "circle",
            "bg-class": "danger",
            "bg-color": "#dc3545",
            "text": DataFrameType.PYSPARK.value,
        },
        "spark_sql": {
            "shape": "circle",
            "bg-class": "info",
            "bg-color": "#2272b4",
            "text": "spark_sql",
        },
        "pandas_on_spark": {
            "shape": "circle",
            "bg-class": "warning",
            "bg-color": "#ffc107",
            "text": DataFrameType.PANDAS_ON_SPARK.value,
        },
        RunStatus.ACTIVE: {"bg-class": "info", "bg-color": "#0dcaf0", "text": "ACTIVE"},
        RunStatus.SKIP: {"bg-class": "dark", "bg-color": "#212529", "text": "SKIPPED"},
    }

    def __init__(self, graph: NodeGraph, width=-1, height=1000):
        self.graph = graph
        self.width = width
        self.height = height
        self._node_positions = self.get_node_positions()
        self._unique_node_names = self.get_unique_node_names()

    def get_unique_node_names(self):
        unique_names = {}
        for key in self._node_positions:
            graph_node = self.graph.get_node(key)
            node_name = graph_node["transformation"].__name__

            if node_name not in list(unique_names.values()):
                unique_names[key] = node_name
            else:
                unique_names[
                    key
                ] = f"{graph_node['transformation'].__module__}.{graph_node['transformation'].__name__}"

        return unique_names

    def html(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))

        nodes = self.nodes
        tags = set()
        for node in nodes:
            tags.update(node["definition"]["tags"])

        css_scripts = {}
        js_scripts = {}
        with open(
            os.path.join(dir_path, "amsify.suggestags.css"), "r", encoding="utf-8"
        ) as f:
            css_scripts["amsify_suggestags"] = f.read()
        with open(
            os.path.join(dir_path, "jquery.amsify.suggestags.js"), "r", encoding="utf-8"
        ) as f:
            js_scripts["amsify_suggestags"] = f.read()
        js_scripts[
            "d3_graph_setup"
        ] = f"""
        var user_width = {self.width};
        var user_height = {self.height};
        var tags = {json.dumps(list(tags))};
        var nodes = {json.dumps(nodes)};
        var links = {json.dumps(self.edges)};
        """
        with open(os.path.join(dir_path, "d3js.js"), "r", encoding="utf-8") as f:
            js_scripts["d3js"] = f.read()
        with open(os.path.join(dir_path, "offcanvas.js"), "r", encoding="utf-8") as f:
            js_scripts["offcanvas"] = f.read()
        with open(os.path.join(dir_path, "tags.js"), "r", encoding="utf-8") as f:
            js_scripts["tags"] = f.read()

        return get_template("index.html").render(
            height=self.height, css_scripts=css_scripts, js_scripts=js_scripts
        )

    def get_node_positions(self):
        """
        Simple algorithm to set the positions of the nodes when printing them. The x coordinate is set directly from the
        depth, to set the y coordinate we are just evenly spacing however many nodes existing at a particular depth.
        """
        nodes_depth = self.graph.get_nodes_depth()

        node_positions = {}
        for depth in sorted(nodes_depth.keys()):
            vertical_padding = round(100 / (len(nodes_depth[depth]) + 1), 2)

            for i, node in enumerate(nodes_depth[depth]):
                x = float(depth)
                # Tweak the y position of nodes in different depths to avoid overlapping lines. For example if we had
                # transformations t1, t2 and t3 with edges between t1 and t2, t1 and t3 and t2 and t3 then the line
                # between t1 and t3 would be hidden
                vertical_displacement = 2.5 if depth % 2 == 0 else 0.0
                y = float((i + 1) * vertical_padding) - vertical_displacement
                node_positions[node] = [x, y]

        return node_positions

    @property
    def nodes(self):
        nodes = []

        for node_name, position in self._node_positions.items():
            graph_node = self.graph.get_node(node_name)

            successors = sorted(list(self.graph.graph.successors(node_name)))
            successors_names = [
                self._unique_node_names[
                    self.graph.get_node(successor)["transformation"].key
                ]
                for successor in successors
            ]

            dependencies = sorted(list(self.graph.graph.predecessors(node_name)))
            dependencies_names = [
                self._unique_node_names[
                    self.graph.get_node(dependency)["transformation"].key
                ]
                for dependency in dependencies
            ]

            node_attributes = {
                "key": graph_node["transformation"].key,
                "name": self._unique_node_names[graph_node["transformation"].key],
                "python_import": None,
                "file_location": None,
                "position": position,
                "active": RunStatus.ACTIVE == graph_node["status"],
                "run_status": GraphHTML.CSS_MAP[graph_node["status"]],
                "type": GraphHTML.CSS_MAP[graph_node["transformation"].type],
                "node_type": graph_node["transformation"].node_type.value,
                "dependencies": dependencies,
                "dependencies_names": dependencies_names,
                "successors": successors,
                "successors_names": successors_names,
                "definition": {
                    "description": graph_node["transformation"].description,
                    "tags": graph_node["transformation"].tags
                    + [graph_node["transformation"].__name__],
                    "columns": self._get_node_columns(node_name),
                },
            }

            if graph_node["transformation"].node_type == NodeType.DATASOURCE:

                if "columns" in node_attributes:
                    node_attributes["definition"]["columns"] = [
                        {"name": column, "type": None, "description": None}
                        for column in graph_node["output_columns"]
                    ]

                node_attributes["definition"]["query"] = {
                    "table": graph_node["transformation"].__name__,
                    "columns": graph_node["output_columns"],
                }
            else:
                if graph_node["transformation"].__package__:
                    node_attributes["python_import"] = (
                        f"from {graph_node['transformation'].__module__} "
                        f"import {graph_node['transformation'].__name__}"
                    )

                node_attributes["file_location"] = graph_node["transformation"].__file__
            nodes.append(node_attributes)
        return nodes

    def _get_node_columns(self, node_name):
        graph_node = self.graph.get_node(node_name)
        if graph_node["transformation"].output_schema:
            columns = [
                {
                    "name": column.name,
                    "type": column.type.__class__.__name__,
                    "description": column.description,
                }
                for column in graph_node["transformation"].output_schema.columns
            ]
        else:
            # If the node has no schema defined then we work out what columns exist based on what dependencies of the
            # node are requesting.
            successors = sorted(list(self.graph.graph.successors(node_name)))
            requested_columns = set()
            for successor in successors:
                successor_dependency_definition = [
                    dependency_definition
                    for dependency_definition in self.graph.get_node(successor)[
                        "transformation"
                    ].input_nodes
                    if dependency_definition.key == node_name
                ][0]
                if successor_dependency_definition.selected_columns:
                    for column in successor_dependency_definition.selected_columns:
                        requested_columns.add(column)
            columns = [
                {
                    "name": column,
                    "type": Unknown.__name__,
                    "description": "",
                }
                for column in sorted(list(requested_columns))
            ]

        return columns

    @property
    def edges(self):
        edges = []
        for source_node_name, target_node_name in self.graph.get_edges():
            source_node = self.graph.get_node(source_node_name)
            target_node = self.graph.get_node(target_node_name)
            edge_data = self.graph.get_edge_data(source_node_name, target_node_name)

            edges.append(
                {
                    "source": source_node["transformation"].key,
                    "source_name": self._unique_node_names[
                        source_node["transformation"].key
                    ],
                    "source_position": self._node_positions[
                        source_node["transformation"].key
                    ],
                    "source_selected_columns": edge_data["selected_columns"],
                    "target": target_node["transformation"].key,
                    "target_name": self._unique_node_names[
                        target_node["transformation"].key
                    ],
                    "target_position": self._node_positions[
                        target_node["transformation"].key
                    ],
                    "active": (
                        # pylint: disable-next=consider-using-in
                        source_node["status"] != RunStatus.SKIP
                        and target_node["status"] != RunStatus.SKIP
                    ),
                }
            )
        return edges
