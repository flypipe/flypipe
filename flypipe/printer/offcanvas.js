const bsOffcanvas = new bootstrap.Offcanvas('#offcanvas', {
    backdrop: true,
    scroll: true,
    keyboard: true,
})

const offcanvasTitle = document.getElementById('offcanvas-title');
const offcanvasBody = document.getElementById('offcanvas-body');

function show_offcanvas(title, body){
    offcanvasTitle.innerHTML = title;
    offcanvasBody.innerHTML = body;
    bsOffcanvas.show();
}

function query_html(node){
    body = ""
    if (node.definition.hasOwnProperty('query')){

        body += "<h6 class='mt-3'>" + node.definition.query.table + "</h6>";
        body += "<code><div class='bg-light vstack gap-1 p-3'><div>SELECT</div>";
        for (let i = 0; i < node.definition.query.columns.length; i++) {
            body += "<div class='ms-3'>" +  node.definition.query.columns[i] + (i != node.definition.query.columns.length - 1? ',':'') + '</div>';
        }
        body += "<div>FROM " + node.definition.query.table + "</div></div></code>";

    }



    return body;
}

function show_raw_queries(nodes){
    body = ""
    has_query = false;
    for (let i = 0; i < nodes.length; i++) {

        if ( nodes[i].definition.hasOwnProperty('query') ){
            has_query = true;
            body += query_html(nodes[i]);
        }
    }
    title = "Raw Queries";
    if ( has_query == false){
        body = "No queries defined by this graph";
    }
    show_offcanvas(title, body);

}

function show_transformation(node){

    if (typeof node === 'string' || node instanceof String) {
        node = nodes_map[node];
    }

    title = node.node_type;

    body = "<h5 class='mb-4'>" + node.name + "</h5>";
    body += '<p class="text-break fw-lighter font-monospace mb-5">' +
                node.definition.description +
           '</p>';

    if (node.file_location){
        body += "<h5 class='mb-4'>Location</h5>";
        body += '<p class="text-break fw-lighter font-monospace mb-5">' +
                    node.file_location +
                '</p>';
    }

    if (node.python_import){
        body += "<h5 class='mb-4'>Python import</h5>";
        body += '<p class="text-break fw-lighter font-monospace mb-5 text-bg-light"><div class="bg-light vstack gap-1 p-3">';
        body += "<code>" + node.python_import + "</code></div></p>";
    }

    body += "<h5 class='mt-5 mb-3'>Tags</h5>";
    for (let i = 0; i < node.definition.tags.length; i++) {
        body += "<span class='badge text-bg-light mr-3'>" + node.definition.tags[i] + "</span>";
    }

    body += "<h5 class='mt-5 mb-3'>Transformation</h5>";
    body += '<ul class="list-unstyled">';
    body += '<li class="list-group-item">Type' + '<span class="badge text-bg-' + node.type['bg-class'] + ' float-end">' + node.type['text'] + '</span></li>';
    body += '<li class="list-group-item">Status' + '<span class="badge text-bg-' + node.run_status['bg-class'] + ' float-end">' + node.run_status['text'] + '</span></li>';
    body += '</ul>';

    body += "<h5 class='mt-5 mb-3'>Dependencies</h5>";
    body += '<ul class="list-unstyled">';
    if (Object.keys(node.dependencies).length == 0){
        body += '<li class="list-group-item fst-italic">No dependencies</li>';
    }
    for (let i = 0; i < node.dependencies.length; i++) {
        dependency_key = "'" + node.dependencies[i] + "'";
        dependency_name = node.dependencies_names[i];
        body += '<li class="list-group-item"><a style="cursor: pointer;" class="link-dark" onclick="show_transformation(' + dependency_key + ');">' + dependency_name + '</a></li>';
        link = get_link(node.dependencies[i], node.key);

        if (link.source_selected_columns) {
            for (let i = 0; i < link.source_selected_columns.length; i++) {
                selected_column = link.source_selected_columns[i];
                body += '<li class="list-group-item"><span class="ms-3">' + selected_column + '</span></li>';
            }
        }

    }

    body += '</ul>';

    body += "<h5 class='mt-5 mb-3'>Successors</h5>";
    body += '<ul class="list-unstyled">';
    if (node.successors.length == 0){
        body += '<li class="list-group-item fst-italic">No successors</li>';
    }
    for (let i = 0; i < node.successors_names.length; i++) {
        successor_name = node.successors_names[i];
        successor_key = "'" + node.successors[i] + "'";
        body += '<li class="list-group-item"><a style="cursor: pointer;" class="link-dark" onclick="show_transformation(' + successor_key + ');">' + successor_name + '</a></li>';
    }
    body += '</ul>';

    if (node.definition.hasOwnProperty('query')){
        body += "<h5 class='mt-5 mb-3'>Raw query</h5>";
        body += query_html(node);
    }


    if (node.definition.columns.length > 0){
        body += "<h5 class='mt-5 mb-3'>Output Schema</h5>";
    }
    for (let i = 0; i < node.definition.columns.length; i++) {
        column = node.definition.columns[i];
        body += '<p class="fw-bold text-start">' +
                    column.name +

                    (column.type ? '<span class="badge rounded-pill text-bg-secondary float-end">' + column.type + '</span>' : '') +
                '</p>' +

                (column.description ?
                    ('<p class="fw-lighter font-monospace mb-5">' + column.description + '</p>')
                    :'');
    }

    show_offcanvas(title, body);
}