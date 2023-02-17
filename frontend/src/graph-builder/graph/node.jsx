import React, { useCallback } from 'react';
import { useReactFlow, Handle, Position } from 'reactflow';
import { refreshNodePositions } from '../util';


const Node = ({ data }) => {
    const graph = useReactFlow();
    const {label} = data;

    const handleConnect = useCallback(({source, target}) => {
        const edgeId = `${source}-${target}`;
        if (!graph.getEdge(edgeId)) {
            graph.addEdges({
                id: edgeId,
                source,
                target
            })
            refreshNodePositions(graph);
        }
    }, [graph]);

    return <>
        <Handle
            type="target"
            position={Position.Left}
            id="target-handle"
            isConnectable
        />
        <div className="p-2 border rounded" draggable>
            {label}
        </div>
        <Handle
            type="source"
            position={Position.Right}
            id="source-handle"
            onConnect={handleConnect}
            isConnectable
        />
    </>
}


export default Node;