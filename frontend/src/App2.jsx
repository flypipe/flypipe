import React, { useCallback } from "react";
import ReactFlow, {
    addEdge,
    MiniMap,
    Controls,
    Background,
    useNodesState,
    useEdgesState,
} from "reactflow";

import "reactflow/dist/style.css";

const initialNodes = [
    {
        id: "1",
        type: "input",
        data: {
            label: "Input Node",
        },
        position: { x: 250, y: 0 },
    },
    {
        id: "2",
        data: {
            label: "Default Node",
        },
        position: { x: 100, y: 100 },
    },
    {
        id: "3",
        type: "output",
        data: {
            label: "Output Node",
        },
        position: { x: 400, y: 100 },
    },
];

const initialEdges = [
    { id: "e1-2", source: "1", target: "2", label: "this is an edge label" },
];

const minimapStyle = {
    height: 120,
};

const onInit = (reactFlowInstance) =>
    console.log("flow loaded:", reactFlowInstance);

const OverviewFlow = () => {
    const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
    const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);
    const onConnect = useCallback(
        (params) => setEdges((eds) => addEdge(params, eds)),
        []
    );

    // we are using a bit of a shortcut here to adjust the edge type
    // this could also be done with a custom edge for example
    const edgesWithUpdatedTypes = edges.map((edge) => {
        if (edge.sourceHandle) {
            const edgeType = nodes.find((node) => node.type === "custom").data
                .selects[edge.sourceHandle];
            edge.type = edgeType;
        }

        return edge;
    });

    return (
        <ReactFlow
            nodes={nodes}
            edges={edgesWithUpdatedTypes}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onInit={onInit}
            fitView
            attributionPosition="top-right"
        >
            <MiniMap style={minimapStyle} zoomable pannable />
            <Controls />
            <Background color="#aaa" gap={16} />
        </ReactFlow>
    );
};

export default OverviewFlow;
