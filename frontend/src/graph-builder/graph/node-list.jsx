import React from 'react';
import { useReactFlow } from 'reactflow';
import ListGroup from "react-bootstrap/ListGroup";


const NodeList = ({nodeIds}) => {
    const graph = useReactFlow();
    const nodes = nodeIds.map(nodeId => graph.getNode(nodeId));
    
    return <ListGroup variant="flush">
        {nodes.map(({id, data}) => <ListGroup.Item key={`node-list-${id}`}>{data.name}</ListGroup.Item>)}
    </ListGroup>;
};


export default NodeList;