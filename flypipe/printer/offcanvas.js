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
        body += '<li class="list-group-item"><a href="#" class="link-dark" onclick="show_transformation(\'' + dependency + '\');">' + dependency + '</a></li>';
    }
    body += '</ul>';

    body += "<h5 class='mt-5 mb-3'>Successors</h5>";
    body += '<ul class="list-unstyled">';
    if (node.successors.length == 0){
        body += '<li class="list-group-item fst-italic">No successors</li>';
    }
    for (let i = 0; i < node.successors.length; i++) {
        successor = node.successors[i];
        body += '<li class="list-group-item"><a href="#" class="link-dark" onclick="show_transformation(\'' + successor + '\');">' + successor + '</a></li>';
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