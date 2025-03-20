import React, { useCallback, useState, useMemo, useContext } from "react";
import ConfirmDelete from "./confirm-delete";
import { NotificationContext } from "../../notifications/context";
import { useReactFlow } from "reactflow";
import { Button } from "react-bootstrap";
import { deleteNode, deleteEdge } from "../../util";

const Delete = ({ buttonText, title, description, handleConfirmDelete }) => {
    const [showConfirm, setShowConfirm] = useState(false);

    const handleClick = useCallback(() => {
        setShowConfirm(true);
    }, [setShowConfirm]);

    const handleCancelDelete = useCallback(() => {
        setShowConfirm(false);
    }, [setShowConfirm]);

    const onSubmit = useCallback(() => {
        handleConfirmDelete();
        setShowConfirm(false);
    }, [handleConfirmDelete, setShowConfirm]);

    return (
        <>
            {showConfirm && (
                <ConfirmDelete
                    title={title}
                    description={description}
                    onCancel={handleCancelDelete}
                    onSubmit={onSubmit}
                />
            )}
            <Button variant="danger" onClick={handleClick}>
                {buttonText}
            </Button>
        </>
    );
};

export const DeleteNode = ({ nodeId, onDelete = () => {} }) => {
    const graph = useReactFlow();
    const { addNotification } = useContext(NotificationContext);
    const nodeName = useMemo(() => {
        const node = graph.getNode(nodeId);
        return node.data.label;
    }, [graph, nodeId]);

    const handleConfirmDelete = useCallback(() => {
        deleteNode(graph, nodeId);
        addNotification(`Node ${nodeName} deleted`);
        onDelete();
    }, [graph, nodeId, nodeName, addNotification, onDelete]);

    return (
        <Delete
            buttonText="Delete"
            title="Delete Node"
            description={`Are you sure you want to delete ${nodeName}?`}
            handleConfirmDelete={handleConfirmDelete}
        />
    );
};

export const DeleteEdge = ({ edgeId, onDelete = () => {} }) => {
    const graph = useReactFlow();
    const { addNotification } = useContext(NotificationContext);

    const handleConfirmDelete = useCallback(() => {
        deleteEdge(graph, edgeId);
        addNotification(`Edge deleted`);
        onDelete();
    }, [graph, edgeId, addNotification, onDelete]);

    return (
        <Delete
            buttonText="Delete"
            title="Delete Edge"
            description={`Are you sure you want to delete this edge?`}
            handleConfirmDelete={handleConfirmDelete}
        />
    );
};

export const ClearGraph = () => {
    const graph = useReactFlow();
    const { addNotification } = useContext(NotificationContext);

    const handleConfirmDelete = useCallback(() => {
        const nodes = graph.getNodes();
        const edges = graph.getEdges();
        graph.deleteElements({ nodes, edges });
        addNotification(`All nodes and edges deleted`);
    }, [graph, addNotification]);

    return (
        <Delete
            buttonText="Clear Graph"
            title="Clear Graph"
            description={`Are you sure you want to delete all nodes and edges?`}
            handleConfirmDelete={handleConfirmDelete}
        />
    );
};
