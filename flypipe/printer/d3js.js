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
const stroke_width = 10;

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
var nodes = [{"name": "table", "position": [1.0, 50.0], "active": true, "run_status": {"bg-class": "info", "bg-color": "#0dcaf0", "text": "ACTIVE"}, "type": {"shape": "circle", "bg-class": "danger", "bg-color": "#dc3545", "text": "PySpark"}, "node_type": "DATASOURCE", "dependencies": [], "successors": ["t0"], "definition": {"description": "Spark table table", "columns": []}}, {"name": "t0", "position": [2.0, 50.0], "active": true, "run_status": {"bg-class": "info", "bg-color": "#0dcaf0", "text": "ACTIVE"}, "type": {"shape": "circle", "bg-class": "danger", "bg-color": "#dc3545", "text": "PySpark"}, "node_type": "TRANSFORMATION", "dependencies": ["table"], "successors": ["t5", "t6"], "definition": {"description": "No description", "columns": [{"name": "dummy", "type": "Decimal", "description": "No description"}]}}, {"name": "t5", "position": [3.0, 50.0], "active": false, "run_status": {"bg-class": "dark", "bg-color": "#212529", "text": "SKIPPED"}, "type": {"shape": "circle", "bg-class": "success", "bg-color": "#198754", "text": "Pandas"}, "node_type": "TRANSFORMATION", "dependencies": ["t0"], "successors": ["t4", "t2"], "definition": {"description": "No description", "columns": [{"name": "dummy", "type": "Integer", "description": "No description"}]}}, {"name": "t4", "position": [4.0, 33.33], "active": false, "run_status": {"bg-class": "dark", "bg-color": "#212529", "text": "SKIPPED"}, "type": {"shape": "circle", "bg-class": "warning", "bg-color": "#ffc107", "text": "Pandas on Spark"}, "node_type": "TRANSFORMATION", "dependencies": ["t5"], "successors": ["t2"], "definition": {"description": "No description", "columns": [{"name": "dummy", "type": "Integer", "description": "No description"}]}}, {"name": "t6", "position": [4.0, 66.67], "active": true, "run_status": {"bg-class": "info", "bg-color": "#0dcaf0", "text": "ACTIVE"}, "type": {"shape": "circle", "bg-class": "success", "bg-color": "#198754", "text": "Pandas"}, "node_type": "TRANSFORMATION", "dependencies": ["t0"], "successors": ["t3", "t7", "t8"], "definition": {"description": "No description", "columns": [{"name": "dummy", "type": "Integer", "description": "No description"}]}}, {"name": "t2", "position": [5.0, 25.0], "active": false, "run_status": {"bg-class": "dark", "bg-color": "#212529", "text": "SKIPPED"}, "type": {"shape": "circle", "bg-class": "success", "bg-color": "#198754", "text": "Pandas"}, "node_type": "TRANSFORMATION", "dependencies": ["t4", "t5"], "successors": ["t1", "t9"], "definition": {"description": "No description", "columns": [{"name": "dummy", "type": "Integer", "description": "No description"}]}}, {"name": "t3", "position": [5.0, 50.0], "active": true, "run_status": {"bg-class": "info", "bg-color": "#0dcaf0", "text": "ACTIVE"}, "type": {"shape": "circle", "bg-class": "danger", "bg-color": "#dc3545", "text": "PySpark"}, "node_type": "TRANSFORMATION", "dependencies": ["t6"], "successors": ["t1"], "definition": {"description": "No description", "columns": [{"name": "dummy", "type": "Integer", "description": "No description"}]}}, {"name": "t7", "position": [5.0, 75.0], "active": true, "run_status": {"bg-class": "info", "bg-color": "#0dcaf0", "text": "ACTIVE"}, "type": {"shape": "circle", "bg-class": "success", "bg-color": "#198754", "text": "Pandas"}, "node_type": "TRANSFORMATION", "dependencies": ["t6"], "successors": ["t1"], "definition": {"description": "No description", "columns": [{"name": "dummy", "type": "Integer", "description": "No description"}]}}, {"name": "t1", "position": [6.0, 50.0], "active": true, "run_status": {"bg-class": "info", "bg-color": "#0dcaf0", "text": "ACTIVE"}, "type": {"shape": "circle", "bg-class": "success", "bg-color": "#198754", "text": "Pandas"}, "node_type": "TRANSFORMATION", "dependencies": ["t2", "t3", "t7"], "successors": ["t8"], "definition": {"description": "No description", "columns": [{"name": "dummy", "type": "Integer", "description": "No description"}]}}, {"name": "t8", "position": [7.0, 33.33], "active": true, "run_status": {"bg-class": "info", "bg-color": "#0dcaf0", "text": "ACTIVE"}, "type": {"shape": "circle", "bg-class": "danger", "bg-color": "#dc3545", "text": "PySpark"}, "node_type": "TRANSFORMATION", "dependencies": ["t1", "t6"], "successors": ["t10"], "definition": {"description": "No description", "columns": [{"name": "dummy", "type": "Integer", "description": "No description"}]}}, {"name": "t9", "position": [7.0, 66.67], "active": true, "run_status": {"bg-class": "info", "bg-color": "#0dcaf0", "text": "ACTIVE"}, "type": {"shape": "circle", "bg-class": "danger", "bg-color": "#dc3545", "text": "PySpark"}, "node_type": "TRANSFORMATION", "dependencies": ["t2"], "successors": ["t10"], "definition": {"description": "No description", "columns": [{"name": "dummy", "type": "Integer", "description": "No description"}]}}, {"name": "t10", "position": [8.0, 50.0], "active": true, "run_status": {"bg-class": "info", "bg-color": "#0dcaf0", "text": "ACTIVE"}, "type": {"shape": "circle", "bg-class": "danger", "bg-color": "#dc3545", "text": "PySpark"}, "node_type": "TRANSFORMATION", "dependencies": ["t8", "t9"], "successors": [], "definition": {"description": "No description", "columns": [{"name": "dummy", "type": "Integer", "description": "No description"}]}}]
var links = [{"source": "t8", "source_position": [7.0, 33.33], "target": "t10", "target_position": [8.0, 50.0], "active": true}, {"source": "t1", "source_position": [6.0, 50.0], "target": "t8", "target_position": [7.0, 33.33], "active": true}, {"source": "t2", "source_position": [5.0, 25.0], "target": "t1", "target_position": [6.0, 50.0], "active": true}, {"source": "t2", "source_position": [5.0, 25.0], "target": "t9", "target_position": [7.0, 66.67], "active": true}, {"source": "t4", "source_position": [4.0, 33.33], "target": "t2", "target_position": [5.0, 25.0], "active": false}, {"source": "t5", "source_position": [3.0, 50.0], "target": "t4", "target_position": [4.0, 33.33], "active": false}, {"source": "t5", "source_position": [3.0, 50.0], "target": "t2", "target_position": [5.0, 25.0], "active": false}, {"source": "t0", "source_position": [2.0, 50.0], "target": "t5", "target_position": [3.0, 50.0], "active": true}, {"source": "t0", "source_position": [2.0, 50.0], "target": "t6", "target_position": [4.0, 66.67], "active": true}, {"source": "table", "source_position": [1.0, 50.0], "target": "t0", "target_position": [2.0, 50.0], "active": true}, {"source": "t3", "source_position": [5.0, 50.0], "target": "t1", "target_position": [6.0, 50.0], "active": true}, {"source": "t6", "source_position": [4.0, 66.67], "target": "t3", "target_position": [5.0, 50.0], "active": true}, {"source": "t6", "source_position": [4.0, 66.67], "target": "t7", "target_position": [5.0, 75.0], "active": true}, {"source": "t6", "source_position": [4.0, 66.67], "target": "t8", "target_position": [7.0, 33.33], "active": true}, {"source": "t7", "source_position": [5.0, 75.0], "target": "t1", "target_position": [6.0, 50.0], "active": true}, {"source": "t9", "source_position": [7.0, 66.67], "target": "t10", "target_position": [8.0, 50.0], "active": true}]


//x and y scales
var xScale = d3.scaleLinear().domain([d3.min(nodes, d => d.position[0]), d3.max(nodes, d => d.position[0])]).range([100, view_port_width * 0.9]);
var yScale = d3.scaleLinear().domain([d3.min(nodes, d => d.position[1]), d3.max(nodes, d => d.position[1])]).range([100, view_port_height * 0.9]);

function node_id(id){ return "node-" + id.replace('.', '-'); }
function link_id(source_id, target_id){ return source_id.replace('.', '-') + "-" + target_id.replace('.', '-'); }
function text_id(id){ return "text-" + id.replace('.', '-'); }

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
    .attr("stroke-width", stroke_width)
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
    .on('mouseover', function (d, i) { highlight_path(d,i); })
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

function highlight_path(d,i){

    d3.selectAll('path.link')
      .filter(function(d_, i) {
        return d_['source'] == d['name'] | d_['target'] == d['name'];;
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
        .attr("x", dragged_node.attr('cx') * 1  - circle_radius)
        .attr("y", dragged_node.attr('cy') * 1  - circle_radius - 5);

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

function suppress_nodes(){
    d3.selectAll("circle").attr("stroke","none").attr("stroke-width",0).attr("r", circle_radius);
}

function highlight_node(node_name){
    d3.select("#" + node_id(node_name)).attr("stroke","black").attr("stroke-width",2).attr("r", circle_radius + 3);
}
