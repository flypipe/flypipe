import React, { memo } from 'react';
import { Handle, Position } from 'reactflow';


const Node = ({ data }) => {
    const {label} = data;
    return <>
        <Handle
            type="target"
            position={Position.Left}
            id="target-handle"
            onConnect={(params) => console.log('handle onConnect', params)}
            isConnectable
        />
        <div className="p-2 border rounded" draggable>
            {label}
        </div>
        <Handle
            type="source"
            position={Position.Right}
            id="source-handle"
            onConnect={(params) => console.log('handle onConnect', params)}
            isConnectable
        />
    </>
}


export default Node;