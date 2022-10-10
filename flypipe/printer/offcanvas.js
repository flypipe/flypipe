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
    for (let i = 0; i < nodes.length; i++) {
        node = nodes[i];
        body += query_html(node);

    }
    title = "Raw Queries";
    show_offcanvas(title, body);

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

    body = "<h5 class='mb-4'>" + node.varname + "</h5>";
    body += '<p class="text-break fw-lighter font-monospace mb-5">' +
                node.definition.description +
           '</p>';

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
        dependency = node.dependencies[i];
        dependency_ = "'" + dependency + "'";
        body += '<li class="list-group-item"><a style="cursor: pointer;" class="link-dark" onclick="show_transformation(' + dependency_ + ');">' + dependency + '</a></li>';
        link = get_link(dependency, node.name);

        for (let i = 0; i < link.source_selected_columns.length; i++) {
            selected_column = link.source_selected_columns[i];
            body += '<li class="list-group-item"><span class="ms-3">' + selected_column + '</span></li>';
        }

    }

    body += '</ul>';

    body += "<h5 class='mt-5 mb-3'>Successors</h5>";
    body += '<ul class="list-unstyled">';
    if (node.successors.length == 0){
        body += '<li class="list-group-item fst-italic">No successors</li>';
    }
    for (let i = 0; i < node.successors.length; i++) {
        successor = node.successors[i];
        successor_ = "'" + successor + "'";
        body += '<li class="list-group-item"><a style="cursor: pointer;" class="link-dark" onclick="show_transformation(' + successor_ + ');">' + successor + '</a></li>';
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