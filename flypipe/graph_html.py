import json

import networkx as nx

from flypipe.node_graph import RunStatus
from flypipe.node_type import NodeType
from flypipe.utils import DataFrameType

class GraphHTML:

    CSS_MAP = {
        DataFrameType.PANDAS: {'shape': 'circle', 'bg-class': 'success', 'bg-color': '#198754', 'text': 'Pandas'},
        DataFrameType.PYSPARK: {'shape': 'circle', 'bg-class': 'danger', 'bg-color': '#dc3545', 'text': 'PySpark'},
        DataFrameType.PANDAS_ON_SPARK: {'shape': 'circle', 'bg-class': 'warning', 'bg-color': '#ffc107', 'text': 'Pandas on Spark'},

        RunStatus.ACTIVE: {'bg-class': 'info', 'bg-color': '#0dcaf0', 'text': 'ACTIVE'},
        RunStatus.SKIP: {'bg-class': 'dark', 'bg-color': '#212529', 'text': 'SKIPPED'},
    }

    @staticmethod
    def html(nodes, links):
        return """
<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="X-UA-Compatible" content="ie=edge">
  <title>Flypipe</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.2.1/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <nav class="navbar fixed-top navbar-expand-lg navbar-dark bg-primary bg-gradient">
      <div class="container-fluid">
        <a class="navbar-brand" href="https://github.com/flypipe/flypipe" target="_blank">Flypipe</a>
        <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarSupportedContent" aria-controls="navbarSupportedContent" aria-expanded="false" aria-label="Toggle navigation">
          <span class="navbar-toggler-icon"></span>
        </button>
        <div class="collapse navbar-collapse" id="navbarSupportedContent">
          <ul class="navbar-nav me-auto mb-2 mb-lg-0">
            <li class="nav-item">
              <a class="nav-link active" aria-current="page" href="https://github.com/flypipe/flypipe" target="_blank">Documentation</a>
            </li>
            <li class="nav-item dropdown">
              <a class="nav-link dropdown-toggle" href="#" role="button" data-bs-toggle="dropdown" aria-expanded="false">
                Actions
              </a>
              <ul class="dropdown-menu">
                <li><a class="dropdown-item" href="#">Raw Queries</a></li>
              </ul>
            </li>
          </ul>
          <form class="d-flex" role="search">
            <input class="form-control me-2" type="search" placeholder="Search" aria-label="Search">
            <button class="btn btn-outline-light" type="submit">Search</button>
          </form>
        </div>
      </div>
    </nav>

    <div class="text-center">
        <div class="row">
            <div class="col">
                <div class="canvas"></div>
            </div>
        </div>
    </div>
    <script src="https://d3js.org/d3.v5.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.2.1/dist/js/bootstrap.bundle.min.js"></script>


    <!--Offcanvas-->
    <div class="offcanvas offcanvas-start" width="600" data-bs-scroll="true" tabindex="-1" id="offcanvas" aria-labelledby="offcanvasWithBothOptionsLabel">
      <div class="offcanvas-header">
        <h5 class="offcanvas-title" id="offcanvas-title"></h5>
        <button type="button" class="btn-close" data-bs-dismiss="offcanvas" aria-label="Close"></button>
      </div>
      <div class="offcanvas-body" id="offcanvas-body"></div>
    </div>

    <!--d3js-->
    <script>
    // Settings
    function getWidth() {
      return Math.max(
        document.body.scrollWidth,
        document.documentElement.scrollWidth,
        document.body.offsetWidth,
        document.documentElement.offsetWidth,
        document.documentElement.clientWidth
      );
    }
    
    function getHeight() {
      return Math.max(
        document.body.scrollHeight,
        document.documentElement.scrollHeight,
        document.body.offsetHeight,
        document.documentElement.offsetHeight,
        document.documentElement.clientHeight
      );
    }
    
    const view_port_width = getWidth();
    const view_port_height = getHeight();
    const link_color = "#999";
    const highlight_color = "black";
    const circle_radius = 6;
    const font_size = 16;
    
    // Setting Canvas and SVG
    const canvas = d3.select(".canvas");
    
    const svg = canvas.append('svg')
                      .attr('height', view_port_height)
                      .attr('width', view_port_width)
                      .attr("class","bg-light");
    
    var g = svg.append("g");
    
    const dragged_link_gen = d3.linkHorizontal()
                .source(d => d.source)
                .target(d => d.target)
                .x(d => d[0] - circle_radius + 3)
                .y(d => d[1]);
    
    //Our larger node data
    var nodes = [
        {'name': 't5', 'position': [1.0, 50.0], 'color': 'black', 'active': true},
        {'name': 't4', 'position': [2.0, 50.0], 'color': 'black', 'active': true},
        {
            'name': 't2', 'position': [3.0, 33.33], 'color': 'black', 'active': true,
            'dependencies': ['t5'],
            'successors': ['t1'],
            'definition': {
                'description': 'here is a description for t2',
                'columns':[
                    {'name': 'col1', 'type': 'String', 'description': 'my description of col 1'},
                    {'name': 'col2', 'type': 'Integer', 'description': 'my description of col 2'}
                ]
            }
    
        },
        {'name': 't3', 'position': [3.0, 66.67], 'color': 'black', 'active': true},
        {
            'name': 't1', 'position': [4.0, 50.0], 'color': 'black', 'active': true,
            'dependencies': ['t2', 't4', 't3'],
            'successors': [],
            'definition': {
                'description': 'here is a description',
                'columns':[
                    {'name': 'col1', 'type': 'String', 'description': 'my description of col 1'},
                    {'name': 'col2', 'type': 'Integer', 'description': 'my description of col 2'}
                ]
            }
        },
    ]
    
    var links = [
        {'source': 't2', 'source_position': [3.0, 33.33], 'target': 't1', 'target_position': [4.0, 50.0]},
        {'source': 't5', 'source_position': [1.0, 50.0], 'target': 't2', 'target_position': [3.0, 33.33]},
        {'source': 't5', 'source_position': [1.0, 50.0], 'target': 't4', 'target_position': [2.0, 50.0]},
        {'source': 't3', 'source_position': [3.0, 66.67], 'target': 't1', 'target_position': [4.0, 50.0]},
        {'source': 't4', 'source_position': [2.0, 50.0], 'target': 't3', 'target_position': [3.0, 66.67]},
        {'source': 't4', 'source_position': [2.0, 50.0], 'target': 't1', 'target_position': [4.0, 50.0]}
    ]
    
    var nodes = """ + json.dumps(nodes) + """;
    var links = """ + json.dumps(links) + """;
    
    
    //x and y scales
    var xScale = d3.scaleLinear().domain([d3.min(nodes, d => d.position[0]), d3.max(nodes, d => d.position[0])]).range([100, view_port_width * 0.9]);
    var yScale = d3.scaleLinear().domain([d3.min(nodes, d => d.position[1]), d3.max(nodes, d => d.position[1])]).range([100, view_port_height * 0.9]);
    
    function node_id(id){ return "node-" + id; }
    function link_id(source_id, target_id){ return source_id + "-" + target_id; }
    function text_id(id){ return "text-" + id; }
    
    // Our link generator with the new .x() and .y() definitions
    var linkGen = d3.linkHorizontal()
        .source(d => d.source_position)
        .target(d => d.target_position)
        .x(d => xScale(d[0]) - circle_radius + 3)
        .y(d => yScale(d[1]));
    
    // Adding the links
    d3.select("g")
          .selectAll("path.horizontal")
          .data(links)
          .enter()
          .append("path")
          .attr("d", linkGen)
          .attr("class", "link")
          .attr("fill", "none")
          .attr("stroke", link_color)
          .attr("source", d => d.source)
          .attr("target", d => d.target)
          .attr('marker-end','url(#arrowhead)')
          .attr("id", d => link_id(d.source, d.target))
          .style("opacity", function(d) {
            if (d.active) {
                return 1;
            }
            else{
                return 0.3;
            }
        });
    
    // Adding Markers
    d3.select("svg")
        .append('defs')
        .append('marker')
        .attr('id', 'arrowhead')
        .attr('viewBox', '-0 -5 10 10')
        .attr('refX', 13)
        .attr('refY', 0)
        .attr('orient', 'auto')
        .attr('markerWidth', 10)
        .attr('markerHeight', 10)
        .attr('xoverflow', 'visible')
        .attr('fill', link_color)
        .style('stroke','none')
        .append('path')
        .attr('d', 'M 0,-5 L 10 ,0 L 0,5')
        ;
    
    // Adding the circle nodes
    d3.select("g")
        .selectAll("path.horizontal")
        .data(nodes)
        .enter()
        .append("circle")
        .attr("cx", d => xScale(d.position[0]))
        .attr("cy", d => yScale(d.position[1]))
        .attr("r", circle_radius + "px")
        .attr("id", d => node_id(d.name))
        .attr("name", d => d.name)
        .attr("fill", d => d.type['bg-color'])
        .attr("cursor", "pointer")
        .style("stroke", "black")
        .style("opacity", function(d) {
            if (d.active) {
                return 1;
            }
            else{
                return 0.3;
            }
        })
        .call(d3.drag()
            .on('start', dragStart)
            .on('drag', dragging)
            .on('end', dragEnd)
          )
        .on('mouseover', function (d, i) { highlight(d,i); })
        .on('mouseout', function (d, i) { suppress(d,i); })
        .on('click', function(d,i){ show_transformation(d);  });
    
    
    // Adding the text nodes
    d3.select("g")
      .selectAll("path.horizontal")
      .data(nodes)
      .enter()
      .append("text")
      .attr("font-size", font_size + "px")
      .attr("text-anchor", "left")
      .attr("id", d => text_id(d.name))
      .attr("x", function(d) {
            return xScale(d.position[0]) - circle_radius;
            })
      .attr("y", function(d) {
            return yScale(d.position[1]) - circle_radius - 5;
            })
      .text(d => d.name)
      .style("opacity", function(d) {
            if (d.active) {
                return 1;
            }
            else{
                return 0.3;
            }
        });
    
    var zoom = d3.zoom()
          .scaleExtent([0, 8])
          .on('zoom', function() {
              g.attr('transform', d3.event.transform);
    });
    
    svg.call(zoom);
    
    
    function suppress(d,i){
        d3.selectAll('path.link')
          .filter(function(d_, i) {
            return d_['source'] == d['name'] | d_['target'] == d['name'];
          })
          .attr("stroke", link_color);
    }
    
    function highlight(d,i){
        d3.selectAll('path.link')
          .filter(function(d_, i) {
            return d_['source'] == d['name'] | d_['target'] == d['name'];
          })
          .attr("stroke", highlight_color);
    }
    
    function move_parent_links(d, dragged_node){
    
        // move parent links
        d3.selectAll('path.link')
          .filter(function(d_, i) {
            return d_['source'] == d['name'] | d_['target'] == d['name'];
          })
          .attr("d", function(d_) {
    
            if (d_['source'] == d['name']){
                var source_node = d3.select("#" + node_id(d_.target));
                var data = {
                'target': [source_node.attr('cx') * 1, source_node.attr('cy') * 1],
                'source': [dragged_node.attr('cx') * 1, dragged_node.attr('cy') * 1],
                };
                return dragged_link_gen(data);
            }
            else {
                var source_node = d3.select("#" + node_id(d_.source));
                var data = {
                'source': [source_node.attr('cx') * 1, source_node.attr('cy') * 1],
                'target': [dragged_node.attr('cx') * 1, dragged_node.attr('cy') * 1],
                };
                return dragged_link_gen(data);
            }
    
          });
    
    }
    
    function dragging(d,i,nodes){
    
        //move Node
        var dragged_node = d3.select(nodes[i])
          .attr("cx", d3.event.x)
          .attr("cy", d3.event.y)
          ;
    
        //move text
        d3.select("#" + text_id(dragged_node.attr('name')))
            .attr("x", dragged_node.attr('cx') * 1 + circle_radius * 2 + 2)
            .attr("y", dragged_node.attr('cy') * 1 + 5)
    
        //move link
        move_parent_links(d, dragged_node)
    
    }
    
    function dragStart(d,i,nodes){
        d3.select(nodes[i])
            .style("stroke", "red")
    }
    
    function dragEnd(d,i,nodes){
        d3.select(nodes[i])
            .style("stroke", "black")
    }


    <!--offcanvas-->
    const bsOffcanvas = new bootstrap.Offcanvas('#offcanvas', {
        backdrop: false,
        scroll: true
    })
    
    const offcanvasTitle = document.getElementById('offcanvas-title');
    const offcanvasBody = document.getElementById('offcanvas-body');
    
    function show_offcanvas(title, body){
        offcanvasTitle.innerHTML = title;
        offcanvasBody.innerHTML = body;
        bsOffcanvas.show();
    }
    
    function show_transformation(node){
        if (typeof node === 'string' || node instanceof String) {
            for (let i = 0; i < nodes.length; i++) {
                if (nodes[i].name == node){
                    node = nodes[i];
                    break;
                }
            }
        }
        
        title = node.node_type;
    
        body = "<h5 class='mb-4'>" + node.name + "</h5>";
        body += '<p class="text-break fw-lighter font-monospace mb-5">' +
                    node.definition.description +
               '</p>';
    
        body += "<h5 class='mt-5 mb-3'>Transformation</h5>";
        body += '<ul class="list-unstyled">';
        body += '<li class="list-group-item">Type' + '<span class="badge text-bg-' + node.type['bg-class'] + ' float-end">' + node.type['text'] + '</span></li>';
        body += '<li class="list-group-item">Status' + '<span class="badge text-bg-' + node.run_status['bg-class'] + ' float-end">' + node.run_status['text'] + '</span></li>';
        body += '</ul>';
    
        body += "<h5 class='mt-5 mb-3'>Dependencies</h5>";
        body += '<ul class="list-unstyled">';
        if (node.dependencies.length == 0){
            body += '<li class="list-group-item fst-italic">No dependencies</li>';
        }
        for (let i = 0; i < node.dependencies.length; i++) {
            dependency = node.dependencies[i];
            body += '<li class="list-group-item"><a href="#" class="link-dark" onclick="show_transformation(\\'' + dependency + '\\');">' + dependency + '</a></li>';
        }
        body += '</ul>';
    
        body += "<h5 class='mt-5 mb-3'>Successors</h5>";
        body += '<ul class="list-unstyled">';
        if (node.successors.length == 0){
            body += '<li class="list-group-item fst-italic">No successors</li>';
        }
        for (let i = 0; i < node.successors.length; i++) {
            successor = node.successors[i];
            body += '<li class="list-group-item"><a href="#" class="link-dark" onclick="show_transformation(\\'' + successor + '\\');">' + successor + '</a></li>';
        }
        body += '</ul>';
    
        body += "<h5 class='mt-5 mb-3'>Output Schema</h5>";
        for (let i = 0; i < node.definition.columns.length; i++) {
            column = node.definition.columns[i];
            body += '<p class="fw-bold text-start">' +
                        column.name +
                        '<span class="badge rounded-pill text-bg-secondary float-end">' + column.type + '</span>' +
                    '</p>' +
                    '<p class="fw-lighter font-monospace mb-5">' +
                    column.description +
                    '</p>';
        }
    
        show_offcanvas(title, body);
    }
    
    </script>
</body>
</html>
                               
            """

    @staticmethod
    def get(graph):

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

            source = edge[0]
            target = edge[1]
            links.append({'source': source,
                          'source_position': nodes_position[source],
                          'target': target,
                          'target_position': nodes_position[target],
                          'active': not (graph.nodes[source]['run_status'] == RunStatus.SKIP and
                                         graph.nodes[target]['run_status'] == RunStatus.SKIP)})


        nodes = []
        for node, position in nodes_position.items():

            node_attributes = {
                'name': node,
                'position': position,
                'active': RunStatus.ACTIVE == graph.nodes[node]['run_status'],
                'run_status': GraphHTML.CSS_MAP[graph.nodes[node]['run_status']],
                'type': GraphHTML.CSS_MAP[graph.nodes[node]['type']],
                'node_type': graph.nodes[node]['node_type'].name,
                'dependencies': list(graph.predecessors(node)),
                'successors': list(graph.successors(node)),
                'definition': {
                    'description': graph.nodes[node]['description'],
                    'columns': [],
                }
            }

            if graph.nodes[node]['output_schema']:
                node_attributes['definition']['columns'] = [
                        {
                            'name': column.name,
                            'type': column.type.__class__.__name__,
                            'description': column.description
                        }
                            for column in graph.nodes[node]['output_schema'].columns
                    ]

            nodes.append(node_attributes)


        return GraphHTML.html(nodes, links)