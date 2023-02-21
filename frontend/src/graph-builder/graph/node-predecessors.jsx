import React from 'react';
import ListGroup from 'react-bootstrap/ListGroup';

export const NodePredecessors = ({ dependencies }) => {   
    
    const nodePredecessors = []
    Object.entries(dependencies).map(([nodePredecessor, columns]) => {
        nodePredecessors.push(<ListGroup.Item key={`predecessor_${nodePredecessor}`}>{nodePredecessor}</ListGroup.Item>)
        
    })
    return (<ListGroup variant="flush">{nodePredecessors}</ListGroup>);
}