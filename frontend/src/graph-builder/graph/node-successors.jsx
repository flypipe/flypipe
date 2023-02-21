import React from 'react';
import ListGroup from 'react-bootstrap/ListGroup';

export const NodeSuccessors = ({ successors }) => {   
    console.log("NodeSuccessors: ",successors)
    const nodesSuccessor = []
    successors.forEach((nodeSuccessor) => {
        nodesSuccessor.push(
            <ListGroup.Item key={`successor_${nodeSuccessor}`}>{nodeSuccessor}</ListGroup.Item>
         )
    })

    return (<ListGroup variant="flush">{nodesSuccessor}</ListGroup>);
}