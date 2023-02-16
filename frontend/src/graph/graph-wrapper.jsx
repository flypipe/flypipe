import React from 'react';
import { ReactFlowProvider } from 'reactflow';
import Graph from './graph';


/*
The purpose of this wrapper is just that all code in graph is underneath the ReactFlowProvider, which allows the 
various ReactFlow hooks to function. 
*/
const GraphWrapper = (props) => {
    return <ReactFlowProvider>
        <Graph {...props}/>
    </ReactFlowProvider>
};

export default GraphWrapper;