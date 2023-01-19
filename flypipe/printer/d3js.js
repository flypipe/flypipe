// variables
var nodes_map = {};

for (let i = 0; i < nodes.length; i++) {
    node = nodes[i];
    nodes_map[node.key] = node;
}

var links_map = {};

for (let i = 0; i < links.length; i++) {
    link = links[i];
    links_map[link.source + "-" + link.target] = link;
}

let highlighted_nodes = new Set();
const view_port_width = getWidth();
const view_port_height = getHeight();
const link_color = "#999";
const highlight_color = "black";
const circle_radius = 7;
const font_size = 17;
const stroke_width = 3;
const circle_stroke = 1;


// Settings
function getWidth() {
  return Math.max(
    user_width,
    document.body.scrollWidth,
    document.documentElement.scrollWidth,
    document.body.offsetWidth,
    document.documentElement.offsetWidth,
    document.documentElement.clientWidth
  );
}

function getHeight() {
  return Math.max(
    user_height,
    document.body.scrollHeight,
    document.documentElement.scrollHeight,
    document.body.offsetHeight,
    document.documentElement.offsetHeight,
    document.documentElement.clientHeight
  );
}

function get_link(source_key, target_key){
    return links_map[source_key + "-" + target_key];
}


// Setting Canvas and SVG
const canvas = d3.select(".canvas");

const svg = canvas.append('svg')
                  .attr('height', view_port_height)
                  .attr('width', view_port_width)
                  ;

var g = svg.append("g");

const dragged_link_gen = d3.linkHorizontal()
            .source(d => d.source)
            .target(d => d.target)
            .x(d => d[0] - circle_radius + 3)
            .y(d => d[1]);



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
      .attr("fill", "none")
      .attr("stroke", link_color)
      .attr("source", d => d.source)
      .attr("target", d => d.target)
      .attr('marker-end','url(#arrowhead)')
      .attr("id", d => link_id(d.source, d.target))
      .attr("cursor", "pointer")
      .attr("stroke-width", stroke_width)
      .attr("class", function(d) {
        if (d.active) {
            return "link link-active";
        }
        else{
            return "link link-inactive";
        }
      })
      .on('mouseover', function (d, i) { highlight_link(d,i); })
      .on('mouseout', function (d, i) { suppress_link(d,i); });

// Adding Markers
d3.select("svg")
    .append('defs')
    .append('marker')
    .attr('id', 'arrowhead')
    .attr('viewBox', '-0 -5 10 10')
    .attr('refX', 13)
    .attr('refY', 0)
    .attr('orient', 'auto')
    .attr('markerWidth', 3)
    .attr('markerHeight', 3)
    .attr('xoverflow', 'visible')
    .attr('fill', link_color)
    .style('stroke','none')
    .attr("stroke-width", 1)
    .append('path')
    .attr('d', 'M 0,-5 L 10 ,0 L 0,5')
    ;

var tooltip = d3.select("body")
    .append("div")
    .style("position", "absolute")
    .style("z-index", "10")
    .style("visibility", "hidden")
    .style("background", "#000")
    .html("a simple tooltip");

// Adding the circle nodes
d3.select("g")
    .selectAll("path.horizontal")
    .data(nodes)
    .enter()
    .append("circle")
    .attr("cx", d => xScale(d.position[0]))
    .attr("cy", d => yScale(d.position[1]))
    .attr("r", circle_radius + "px")
    .attr("id", d => node_id(d.key))
    .attr("name", d => d.key)
    .attr("fill", d => d.type['bg-color'])
    .attr("cursor", "pointer")
    .style("stroke", "black")
    .style("stroke-width", circle_stroke)
    .call(d3.drag().on('drag', dragging))
    .on("mouseover", function(d){
        tooltip.html("<strong>" + d.name + "</strong><br/>(" + d.type.text + ")");
        return tooltip.style("visibility", "visible");
    })
    .on("mousemove", function(){
        return tooltip.style("top", (d3.event.pageY-10)+"px").style("left",(d3.event.pageX+10)+"px");
    })
    .on("mouseout", function(){
        return tooltip.style("visibility", "hidden");
    })
    .on('click', function(d,i){ show_transformation(d);  });



function show_node_tooltip(d,i){
    tooltip.text(d);
    return tooltip.style("visibility", "visible");
}

// Adding the text nodes
d3.select("g")
  .selectAll("path.horizontal")
  .data(nodes)
  .enter()
  .append("text")
  .attr("style", "cursor: pointer")
  .attr("font-size", font_size + "px")
  .attr("text-anchor", "left")
  .attr("id", d => text_id(d.key))

  .attr("x", function(d) {
        return xScale(d.position[0]) - circle_radius;
        })
  .attr("y", function(d) {
        return yScale(d.position[1]) - circle_radius - 5;
        })

  .text(d => d.name)
  .call(d3.drag().on('drag', dragging))
  .on("mouseover", function(d){
        tooltip.html("<strong>" + d.name + "</strong><br/>(" + d.type.text + ")");
        return tooltip.style("visibility", "visible");
    })
    .on("mousemove", function(){
        return tooltip.style("top", (d3.event.pageY-10)+"px").style("left",(d3.event.pageX+10)+"px");
    })
    .on("mouseout", function(){
        return tooltip.style("visibility", "hidden");
    })
    .on('click', function(d,i){ show_transformation(d);  })
    ;

var zoom = d3.zoom()
      .scaleExtent([0, 8])
      .on('zoom', function() {
          g.attr('transform', d3.event.transform);
});

svg.call(zoom);

function mouse_position(){
    var coordinates= d3.mouse(this);
    var x = coordinates[0];
    var y = coordinates[1];

    return [x, y];
}

function suppress(d,i){
    d3.selectAll('path.link')
      .filter(function(d_, i) {
        return d_['source'] == d['name'] | d_['target'] == d['name'];
      })
      .attr("stroke", link_color);
}

var tooltip = d3.select("body")
    .append("div")
    .attr("id", "tooltip")
    .style("position", "absolute")
    .style("z-index", "10")
    .style("visibility", "hidden")
    .attr("class", "text-bg-light p-3")
    .text("a simple tooltip");

function suppress_link(d,i){
    d3.selectAll('path.link')
      .filter(function(d_, i) {
        return d_['source'] == d['source'] & d_['target'] == d['target'];
      })
      .attr("stroke", link_color);

    d3.select("#tooltip")
      .style("visibility", "hidden");
}



function highlight_link(d,i){

    d3.selectAll('path.link')
      .filter(function(d_, i) {
            return d_['source'] == d['source'] & d_['target'] == d['target'];
      })
      .attr("stroke", highlight_color)
      ;

    var source = nodes_map[d['source']];
    var target = nodes_map[d['target']];

    var source_target_columns = {};
    if (source.definition.columns.length > 0) {
        // If a schema is defined
        for (let i = 0; i < source.definition.columns.length; i++) {
          source_column = source.definition.columns[i]['name'];
          source_target_columns[source_column] = (!d.source_selected_columns) || d.source_selected_columns.includes(source_column);
        }
    } else {
        // No schema defined on the node
        if (d.source_selected_columns) {
            for (let i = 0; i < d.source_selected_columns.length; i++) {
                source_target_columns[d.source_selected_columns[i]] = true
            }
        }
    }


    var table_body = "";
    if (Object.keys(source_target_columns).length == 0) {
        table_body = '<tr><td>Unknown</td><td><i class="fa fa-long-arrow-right" aria-hidden="true"></i></td><td>Unknown</td></tr>';
    } else {
        for (const [key, value] of Object.entries(source_target_columns)) {
            table_body += "<tr><td>" + key + "</td>";
            table_body += "<td>" + (value? '<i class="fa fa-long-arrow-right" aria-hidden="true"></i>': "") + "</td>";
            table_body += "<td>" + (value? key: "") + "</td></tr>";
        }
    }
    text = `
    <table class="table">
      <thead>
        <tr>
          <th scope="col">` + source.name + `</th>
          <th scope="col"></th>
          <th scope="col">` + target.name + `</th>
        </tr>
      </thead>
      <tbody>`
      + table_body +
      `</tbody></table>`;

    d3.select("#tooltip")
      .style("left", (d3.event.x + 20) + "px")
      .style("top", (d3.event.y + 5) + "px")
      .style("visibility", "visible")
      .html(text);

}

//function highlight_path(d,i){
//
//    d3.selectAll('path.link')
//      .filter(function(d_, i) {
//        return d_['source'] == d['name'] | d_['target'] == d['name'];;
//      })
//      .attr("stroke", highlight_color);
//}

function move_parent_links(d, dragged_node){

    // move parent links
    d3.selectAll('path.link')
      .filter(function(d_, i) {
        return d_['source'] == d['key'] | d_['target'] == d['key'];
      })
      .attr("d", function(d_) {

        if (d_['source'] == d['key']){
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

    //move circle
    d3.select("#" + node_id(d.key))
        .attr("cx", d3.event.x)
        .attr("cy", d3.event.y);

    //move text
    d3.select("#" + text_id(d.key))
        .attr("x", d3.event.x * 1  - circle_radius)
        .attr("y", d3.event.y * 1  - circle_radius - 5);

    //move link
    var dragged_node = d3.select("#" + node_id(d.key));
    move_parent_links(d, dragged_node);

}

function suppress_node(node_name){
    highlighted_nodes.delete(node_name);
    d3.select("#" + node_id(node_name))
    .style("stroke-width", circle_stroke)
    .attr("r", circle_radius);

    d3.select("#" + text_id(node_name))
      .classed('fw-bolder', false);

}

function highlight_node(node_name){
    d3.select("#" + node_id(node_name))
    .style("stroke-width", circle_stroke+1)
    highlighted_nodes.add(node_name);

    d3.select("#" + text_id(node_name))
      .classed('fw-bolder', true);

    d3.select("#" + node_id(node_name))
            .attr('opacity',1)
            .attr("r", circle_radius)
            .transition()
            .duration(350)
            .attr('opacity',0)
            .attr("r", circle_radius * 10)
            .on('end',function(d) { blink(d.key, 0);});

}

//blink
function blink(node_name, o) {
    if (highlighted_nodes.has(node_name)){
        d3.select("#" + node_id(node_name))
            .attr('opacity',o)
            .attr("r", circle_radius)
            .transition()
            .duration(100)
            .attr('opacity',(o == 0.5? 1 : 0.5))
            .on('end',function(d) { blink(d.key, (o == 0.5? 1 : 0.5));});
    }
    else {
        d3.select("#" + node_id(node_name)).attr('opacity',1);
    }
};