import React from "react";
import { useReactFlow } from "reactflow";
import ListGroup from "react-bootstrap/ListGroup";

const NodeList = ({ nodeIds }) => {
    const graph = useReactFlow();
    const graphNodeIds = new Set(graph.getNodes().map(({ id }) => id));
    // We only show the nodes that are present in the graph
    const nodes = nodeIds
        .filter((nodeId) => graphNodeIds.has(nodeId))
        .map((nodeId) => graph.getNode(nodeId));

    return (
        <ListGroup variant="flush">
            {nodes.map(({ id, data }) => (
                <ListGroup.Item key={`node-list-${id}`}>
                    {data.name}
                </ListGroup.Item>
            ))}
        </ListGroup>
    );
};

export default NodeList;
