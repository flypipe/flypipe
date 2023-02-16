import React from 'react';
import { ReactFlowProvider } from 'reactflow';
import Search from './search/search';
import Graph from './graph/graph';


const GraphBuilder = () => {
    
    return <ReactFlowProvider>
        <div className="d-flex col">
            <div className="d-flex flex-column col-4 m-4">
                <Search/>
            </div>
            <div className="col-7">
                <Graph nodeDefs={nodes}/>
            </div>
        </div>
    </ReactFlowProvider>
};


export default GraphBuilder;