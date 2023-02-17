import React, { useCallback, useMemo } from 'react';
import { useReactFlow, Handle, Position } from 'reactflow';
import { refreshNodePositions } from '../util';
import classNames from 'classnames';


const BaseNode = ({ data, isNewNode }) => {
    const graph = useReactFlow();
    const { label } = data;

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

    const klass = useMemo(() => classNames(
        "p-2",
        "border",
        isNewNode ? "rounded" : "",
        {
            "border-3": isNewNode
        }
    ), [isNewNode]);

    return <>
        <Handle
            type="target"
            position={Position.Left}
            id="target-handle"
            isConnectable
        />
        
        <div className={klass} draggable>
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

const ExistingNode = (props) => <BaseNode isNewNode={false} {...props}/>
const NewNode = (props) => <BaseNode isNewNode={true} {...props}/>

export {ExistingNode, NewNode};