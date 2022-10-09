import json
import os

import networkx as nx

from flypipe.node_graph import RunStatus
from flypipe.node_type import NodeType
from flypipe.utils import DataFrameType


class GraphHTML:

    CSS_MAP = {
        DataFrameType.PANDAS: {'shape': 'circle', 'bg-class': 'success', 'bg-color': '#198754', 'text': DataFrameType.PANDAS.value},
        DataFrameType.PYSPARK: {'shape': 'circle', 'bg-class': 'danger', 'bg-color': '#dc3545', 'text': DataFrameType.PYSPARK.value},
        DataFrameType.PANDAS_ON_SPARK: {'shape': 'circle', 'bg-class': 'warning', 'bg-color': '#ffc107', 'text': DataFrameType.PANDAS_ON_SPARK.value},

        RunStatus.ACTIVE: {'bg-class': 'success', 'bg-color': '#198754', 'text': 'ACTIVE'},
        RunStatus.SKIP: {'bg-class': 'dark', 'bg-color': '#212529', 'text': 'SKIPPED'},
    }

    @staticmethod
    def html(nodes, links, width, height):
        dir_path = os.path.dirname(os.path.realpath(__file__))

        tags = set()
        for node in nodes:
            tags.update(node['definition']['tags'])
        index = open(os.path.join(dir_path, "index.html"))
        html = index.read()
        index.close()

        html = html.replace('<div class="text-center" style="height:1000px;">',f'<div class="text-center" style="height:{height}px;">')

        html = html.replace('<link rel="stylesheet" type="text/css" href="amsify.suggestags.css">',
                            f'<style>{open(os.path.join(dir_path, "amsify.suggestags.css")).read()}</style>')
        html = html.replace('<script type="text/javascript" src="jquery.amsify.suggestags.js"></script>',
                            f'<script>{open(os.path.join(dir_path, "jquery.amsify.suggestags.js")).read()}</script>')

        html = html.replace('<script src="data.js"></script>', f'''
        <script>
        var user_width = {width};
        var user_height = {height};
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
    def get(graph, width=-1, height=1000):

        root_node = [node[0] for node in graph.out_degree if node[1] == 0][0]

        nodes_depth = {}
        for node in graph:
            depth = len(max(list(nx.all_simple_paths(graph, node, root_node)), key=lambda x: len(x), default=[root_node]))

            if depth not in nodes_depth:
                nodes_depth[depth] = [node]
            else:
                nodes_depth[depth] += [node]

        max_depth = max(nodes_depth.keys())

        nodes_depth = {-1*k+max_depth+1: v for k,v in nodes_depth.items()}

        nodes_position = {}
        for depth in sorted(nodes_depth.keys()):
            padding = 100/(len(nodes_depth[depth]) + 1)

            for i, node in enumerate(nodes_depth[depth]):

                x = float(depth)
                y = float(round((i+1) * padding, 2))
                nodes_position[node] = [x, y]

        links = []
        for edge in graph.edges:

            source = graph.nodes[edge[0]]
            target = graph.nodes[edge[1]]
            edge_data = graph.get_edge_data(edge[0], edge[1])

            links.append({'source': source['name'],
                          'source_position': nodes_position[source['name']],
                          'source_selected_columns': edge_data['selected_columns'],
                          'target': target['name'],
                          'target_position': nodes_position[target['name']],
                          'active': (not (
                              (source['run_status'] == RunStatus.SKIP and
                               target['run_status'] == RunStatus.SKIP)
                               or target['run_status'] == RunStatus.SKIP)
                                     )})

        nodes = []
        for node, position in nodes_position.items():
            graph_node = graph.nodes[node]
            tags = [node, graph_node['type'].value, graph_node['node_type'].value] + graph_node['tags']

            node_attributes = {
                'name': node,
                'position': position,
                'active': RunStatus.ACTIVE == graph_node['run_status'],
                'run_status': GraphHTML.CSS_MAP[graph_node['run_status']],
                'type': GraphHTML.CSS_MAP[graph_node['type']],
                'node_type': graph_node['node_type'].value,
                'dependencies': sorted(list(graph.predecessors(node))),
                'successors': sorted(list(graph.successors(node))),
                'definition': {
                    'description': graph_node['description'],
                    'tags': tags,
                    'columns': [],
                }
            }

            if graph_node['output_schema']:
                node_attributes['definition']['columns'] = [
                        {
                            'name': column.name,
                            'type': column.type.__class__.__name__,
                            'description': column.description
                        }
                            for column in graph_node['output_schema'].columns
                    ]

            if graph_node['node_type'] == NodeType.DATASOURCE:
                node_attributes['definition']['query'] = {"table":  graph_node['name'],
                                                             "columns": graph_node['selected_columns']}

            nodes.append(node_attributes)



        return GraphHTML.html(nodes, links, width=width, height=height)