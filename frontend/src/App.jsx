import React, { useContext } from "react";
import { ReactFlowProvider } from "reactflow";
import Graph from "./graph/graph";
import { GraphContextProvider } from "./graph/graph-context";
import { NodeDetailsContextProvider } from "./node-details/context";
import { NodeDetails } from "./node-details/node-details";
import Notifications from "./notifications/notifications";
import { NotificationContext } from "./notifications/context";

const App = () => {
    const { newMessage } = useContext(NotificationContext);

    return (
        <>
            <Notifications newMessage={newMessage} />
            <div className="d-flex w-100 h-100">
                <ReactFlowProvider>
                    <GraphContextProvider>
                        <NodeDetailsContextProvider>
                            <NodeDetails />
                            <div className="col">
                                <div>
                                    <Graph
                                        initialNodes={initialNodes}
                                        nodeDefs={nodes}
                                        groupDefs={groups}
                                        tagSuggestions={tagSuggestions}
                                    />
                                </div>
                            </div>
                        </NodeDetailsContextProvider>
                    </GraphContextProvider>
                </ReactFlowProvider>
            </div>
        </>
    );
};

export default App;
