import React, { memo } from 'react';
import { Handle, Position } from 'reactflow';
import { useStore } from './store';


const Node = ({ data }) => {
    const {label} = data;
    const {addEdge} = useStore(({addEdge}) => ({
        addEdge
    }));

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
            onConnect={({source, target}) => {
                addEdge({
                    id: `${source}-${target}`,
                    source,
                    target
                });
            }}
            isConnectable
        />
    </>
}


export default Node;