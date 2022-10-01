import json

import networkx as nx
from flypipe.utils import DataFrameType

class GraphHTML:

    @staticmethod
    def html(nodes, links):
        return """
            <html lang="en">
            <head>
              <meta charset="UTF-8">
              <meta name="viewport" content="width=device-width, initial-scale=1.0">
              <meta http-equiv="X-UA-Compatible" content="ie=edge">
              <title>SVG Basics</title>
            </head>
            <body>

                <div class="canvas"></div>
                <script src="https://d3js.org/d3.v5.min.js"></script>
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
                                      .style("background", "#fbfbfb");

                    var g = svg.append("g");

                    const dragged_link_gen = d3.linkHorizontal()
                                .source(d => d.source)
                                .target(d => d.target)
                                .x(d => d[0] - circle_radius + 3)
                                .y(d => d[1]);

                    //Our larger node data
                    var nodes = """ + json.dumps(nodes) + """
                    var links = """ + json.dumps(links) + """


                    //x and y scales
                    var xScale = d3.scaleLinear().domain([d3.min(nodes, d => d.position[0]), d3.max(nodes, d => d.position[0])]).range([100, view_port_width * 0.8]);
                    var yScale = d3.scaleLinear().domain([d3.min(nodes, d => d.position[1]), d3.max(nodes, d => d.position[1])]).range([100, view_port_height * 0.8]);

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
                          .attr("id", d => link_id(d.source, d.target));

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
                        .attr("fill", d => d.color)
                        .style("stroke", "black")
                        .call(d3.drag()
                            .on('start', dragStart)
                            .on('drag', dragging)
                            .on('end', dragEnd)
                          )
                        .on('mouseover', function (d, i) { highlight(d,i); })
                        .on('mouseout', function (d, i) { suppress(d,i); });


                    // Adding the text nodes
                    d3.select("g")
                      .selectAll("path.horizontal")
                      .data(nodes)
                      .enter()
                      .append("text")
                      .attr("font-size", font_size + "px")
                      .attr("text-anchor", "middle")
                      .attr("id", d => text_id(d.name))
                      .attr("x", function(d) {
                            return xScale(d.position[0])  + circle_radius * 2 + 2;
                            })
                      .attr("y", function(d) {
                            return yScale(d.position[1]) + 5;
                            })
                      .text(d => d.name);

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
                    </script>
            </body>
            </html>        
            """

    @staticmethod
    def get(graph):
        print(graph)
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
                          'target_position': nodes_position[target]})


        nodes = []
        node_type_colors = {
            DataFrameType.PYSPARK: "red",
            DataFrameType.PANDAS: "blue",
            DataFrameType.PANDAS_ON_SPARK: "green"
        }
        for node, position in nodes_position.items():
            nodes.append({'name': node, 'position': position, 'color': node_type_colors[graph.nodes[node]['type']]})

        return GraphHTML.html(nodes, links)