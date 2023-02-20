import React from 'react';
import ListGroup from 'react-bootstrap/ListGroup';

export const NodeSuccessors = ({ successors }) => {   
    const successor = []
    console.log("NodeSuccessors=>",successors)

    successors.forEach((out) => {
        successor.push(
            <ListGroup.Item key={`successor_${out}`}>{out}</ListGroup.Item>
         )
    })

    return (<ListGroup variant="flush">{successor}</ListGroup>);
}