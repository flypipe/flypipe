import React from "react";
import { ReactFlowProvider } from "reactflow";
import Graph from "./graph/graph";
import { GraphContextProvider } from "./graph/graph-context";
import { NodeDetailsContextProvider } from "./node-details/context";
import { NodeDetails } from "./node-details/node-details";

const GraphBuilder = () => {
    return (
        <ReactFlowProvider>
            <GraphContextProvider>
                <NodeDetailsContextProvider>
                    <NodeDetails/>
                    <div className="col">
                        <div>
                            <Graph
                                nodeDefs={nodes}
                                tagSuggestions={tagSuggestions}
                            />
                        </div>
                    </div>
                </NodeDetailsContextProvider>
            </GraphContextProvider>
        </ReactFlowProvider>
    );
};

export default GraphBuilder;
