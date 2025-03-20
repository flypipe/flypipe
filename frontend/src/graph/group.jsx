import React, { useEffect, useState, useCallback } from "react";
import { BsPlusLg, BsDashLg } from "react-icons/bs";
import { AiOutlinePlusCircle } from "react-icons/ai";

import { Handle, Position, useReactFlow } from "reactflow";
import { Badge } from "react-bootstrap";
import Group from "../group";
import { refreshNodePositions } from "../util";

const GroupNode = ({ id, data }) => {
    const graph = useReactFlow();
    const groupId = id;
    const { label, isMinimised } = data;

    const onMaximiseMinimise = useCallback(
        (e) => {
            e.stopPropagation();
            const nodes = graph.getNodes();
            const group = nodes.find(({ id }) => id === groupId);
            group.data.isMinimised = !group.data.isMinimised;
            graph.setNodes(nodes);
            refreshNodePositions(graph);
        },
        [graph, groupId, isMinimised]
    );
    return (
        <div
            className="w-100 h-100 bg-light"
            style={{
                border: "0.1em solid grey",
            }}
        >
            <Handle
                type="target"
                position={Position.Left}
                id="target-handle"
                isConnectable={false}
            />
            <div className="d-flex justify-content-center">
                <span
                    className={"mb-0 me-2 h3"}
                    style={{ textAlign: "center" }}
                >
                    {label}
                </span>
            </div>
            <Badge
                pill
                bg="light"
                className={`align-self-start fs-6 position-absolute node-badge group-expand-collapse`}
                title={isMinimised ? "Expand" : "Collapse"}
                size="md"
                onClick={onMaximiseMinimise}
            >
                {isMinimised ? (
                    <BsPlusLg className="fs-6" fill="dark" />
                ) : (
                    <BsDashLg className="fs-6" fill="dark" />
                )}
            </Badge>
            <Handle
                type="source"
                position={Position.Right}
                id="source-handle"
                isConnectable={false}
            />
        </div>
    );
};

export default GroupNode;
