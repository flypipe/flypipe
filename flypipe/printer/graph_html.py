import json
import os

import networkx as nx

from flypipe.node_graph import RunStatus, NodeGraph
from flypipe.node_type import NodeType
from flypipe.utils import DataFrameType


class GraphHTML:

    CSS_MAP = {
        DataFrameType.PANDAS: {'shape': 'circle', 'bg-class': 'success', 'bg-color': '#198754', 'text': DataFrameType.PANDAS.value},
        DataFrameType.PYSPARK: {'shape': 'circle', 'bg-class': 'danger', 'bg-color': '#dc3545', 'text': DataFrameType.PYSPARK.value},
        DataFrameType.PANDAS_ON_SPARK: {'shape': 'circle', 'bg-class': 'warning', 'bg-color': '#ffc107', 'text': DataFrameType.PANDAS_ON_SPARK.value},

        RunStatus.ACTIVE: {'bg-class': 'info', 'bg-color': '#0dcaf0', 'text': 'ACTIVE'},
        RunStatus.SKIP: {'bg-class': 'dark', 'bg-color': '#212529', 'text': 'SKIPPED'},
    }

    def __init__(self, graph: NodeGraph, width=-1, height=1000):
        self.graph = graph
        self.width = width
        self.height = height

    def html(self, nodes, links):
        dir_path = os.path.dirname(os.path.realpath(__file__))

        tags = set()
        for node in nodes:
            tags.update(node['definition']['tags'])
        index = open(os.path.join(dir_path, "index.html"))
        html = index.read()
        index.close()

        html = html.replace('<div class="text-center" style="height:1000px;">',f'<div class="text-center" style="height:{self.height}px;">')

        html = html.replace('<link rel="stylesheet" type="text/css" href="amsify.suggestags.css">',
                            f'<style>{open(os.path.join(dir_path, "amsify.suggestags.css")).read()}</style>')
        html = html.replace('<script type="text/javascript" src="jquery.amsify.suggestags.js"></script>',
                            f'<script>{open(os.path.join(dir_path, "jquery.amsify.suggestags.js")).read()}</script>')

        html = html.replace('<script src="data.js"></script>', f'''
        <script>
        var user_width = {self.width};
        var user_height = {self.height};
        var tags = {json.dumps(list(tags))};
        var nodes = {json.dumps(nodes)};
        var links = {json.dumps(links)};
        </script>
        ''')
        html = html.replace('<script src="d3js.js"></script>', f'<script>{open(os.path.join(dir_path, "d3js.js")).read()}</script>')
        html = html.replace('<script src="offcanvas.js"></script>', f'<script>{open(os.path.join(dir_path, "offcanvas.js")).read()}</script>')
        html = html.replace('<script src="tags.js"></script>', f'<script>{open(os.path.join(dir_path, "tags.js")).read()}</script>')


        return html

    @staticmethod
    def _nodes_position(graph):
        nodes_depth = graph.get_nodes_depth()

        nodes_position = {}
        for depth in sorted(nodes_depth.keys()):
            padding = 100 / (len(nodes_depth[depth]) + 1)

            for i, node in enumerate(nodes_depth[depth]):
                x = float(depth)
                y = float(round((i + 1) * padding, 2)) - (2.5 if depth % 2 == 0 else 0.0)
                nodes_position[node] = [x, y]

        return nodes_position

    def get(self):
        nodes_position = self._nodes_position(self.graph)

        links = []
        for source_node_name, target_node_name in self.graph.get_edges():
            source_node = self.graph.get_node(source_node_name)
            target_node = self.graph.get_node(target_node_name)
            edge_data = self.graph.get_edge_data(source_node_name, target_node_name)

            links.append({'source': source_node['transformation'].__name__,
                          'source_position': nodes_position[source_node['transformation'].__name__],
                          'source_selected_columns': edge_data['selected_columns'],
                          'target': target_node['transformation'].__name__,
                          'target_position': nodes_position[target_node['transformation'].__name__],
                          'active': (source_node['run_status'] != RunStatus.SKIP and target_node['run_status'] != RunStatus.SKIP)
                          })

        nodes = []
        for node_name, position in nodes_position.items():
            graph_node = self.graph.get_node(node_name)
            tags = (
                [
                    node_name,
                    graph_node['transformation'].type.value,
                    graph_node['transformation'].node_type.value
                ] + graph_node['transformation'].tags
            )

            node_attributes = {
                'name': graph_node['transformation'].__name__,
                'varname': graph_node['transformation'].varname,
                'position': position,
                'active': RunStatus.ACTIVE == graph_node['run_status'],
                'run_status': GraphHTML.CSS_MAP[graph_node['run_status']],
                'type': GraphHTML.CSS_MAP[graph_node['transformation'].type],
                'node_type': graph_node['transformation'].node_type.value,
                'dependencies': sorted(list(self.graph.graph.predecessors(node_name))),
                'successors': sorted(list(self.graph.graph.successors(node_name))),
                'definition': {
                    'description': graph_node['transformation'].description,
                    'tags': tags,
                    'columns': [],
                }
            }

            if graph_node['transformation'].output_schema:
                node_attributes['definition']['columns'] = [
                        {
                            'name': column.name,
                            'type': column.type.__class__.__name__,
                            'description': column.description
                        }
                            for column in graph_node['transformation'].output_schema.columns
                    ]

            if graph_node['transformation'].node_type == NodeType.DATASOURCE:

                node_attributes['definition']['columns'] = [
                    {
                        'name': column,
                        'type': None,
                        'description': None
                    }
                    for column in graph_node['transformation'].selected_columns
                ]

                node_attributes['definition']['query'] = {
                    "table":  graph_node['transformation'].varname,
                    "columns": graph_node['transformation'].selected_columns
                }

            nodes.append(node_attributes)

        return self.html(nodes, links)