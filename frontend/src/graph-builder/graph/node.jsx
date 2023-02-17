import React, { useCallback, useMemo } from 'react';
import { useReactFlow, Handle, Position } from 'reactflow';
import Badge from 'react-bootstrap/Badge';
import { refreshNodePositions } from '../util';
import classNames from 'classnames';


const BaseNode = ({ data, isNewNode, width, height }) => {
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
        "d-flex",
        "justify-content-between",
        "px-4",
        "py-2",
        "border",
        "border-3",
        "rounded",
    ));

    return <>
        <Handle
            type="target"
            position={Position.Left}
            id="target-handle"
            isConnectable
        />
        <div className={klass} style={{width, height}}>
            <p className="mb-0 me-2 h1">{label}</p>
            {isNewNode && <Badge pill bg="primary" className="align-self-start"><span class="fs-6">New</span></Badge>}
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