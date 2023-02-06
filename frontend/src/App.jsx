import React from 'react';


const App = () => {
    return (
        <nav class="navbar fixed-top navbar-expand-lg navbar-dark bg-primary bg-gradient"
            style={{backgroundColor: "#6EACE7 !important;"}}>
            <div class="container-fluid">
                <a class="navbar-brand" href="https://github.com/flypipe/flypipe" target="_blank">Flypipe</a>
                <button class="navbar-toggler" type="button" data-bs-toggle="collapse"
                        data-bs-target="#navbarSupportedContent" aria-controls="navbarSupportedContent"
                        aria-expanded="false" aria-label="Toggle navigation">
                    <span class="navbar-toggler-icon"></span>
                </button>
                <div class="collapse navbar-collapse" id="navbarSupportedContent">
                    <ul class="navbar-nav me-auto mb-2 mb-lg-0">
                        <li class="nav-item">
                            <a class="nav-link active" aria-current="page" href="https://github.com/flypipe/flypipe"
                            target="_blank">Documentation</a>
                        </li>
                <li class="nav-item dropdown">
                <a class="nav-link dropdown-toggle" href="#" role="button" data-bs-toggle="dropdown" aria-expanded="false">
                    Actions
                </a>
                <ul class="dropdown-menu">
                    <li><a class="dropdown-item" style={{cursor: "pointer"}} onclick="show_raw_queries(nodes);">Raw Queries</a></li>
                </ul>
                </li>
            </ul>
            <form class="d-flex" role="search">
                <input id="jose" type="text" class="form-control" name="tags" value=""/>
            </form>
            </div>
        </div>
        </nav>
    )
};

export default App;