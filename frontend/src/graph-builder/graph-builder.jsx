import React from "react";
import { ReactFlowProvider } from "reactflow";
import Graph from "./graph/graph";
import { GraphContextProvider } from "./graph/graph-context";

const GraphBuilder = () => {
    return (
        <ReactFlowProvider>
            <GraphContextProvider>
                <div className="col">
                    <div>
                        <Graph
                            nodeDefs={nodes}
                            tagSuggestions={tagSuggestions}
                        />
                    </div>
                </div>
            </GraphContextProvider>
        </ReactFlowProvider>
    );
};

export default GraphBuilder;
