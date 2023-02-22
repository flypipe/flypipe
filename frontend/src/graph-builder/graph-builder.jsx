import React from "react";
import { ReactFlowProvider } from "reactflow";
import Graph from "./graph/graph";

const GraphBuilder = () => {
    return (
        <ReactFlowProvider>
            <div className="col">
                <div>
                    <Graph nodeDefs={nodes} tagSuggestions={tagSuggestions} />
                </div>
            </div>
        </ReactFlowProvider>
    );
};

export default GraphBuilder;
