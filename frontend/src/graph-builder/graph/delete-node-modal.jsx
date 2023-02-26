import React, { useMemo, useCallback, useContext } from "react";
import { Button, Form, Modal } from "react-bootstrap";
import { useReactFlow } from "reactflow";
import { NotificationContext } from "../../context";
import { deleteNode } from "../util";

const DeleteNodeModal = ({ nodeId, onCancel, onSubmit }) => {
    const { addNotification } = useContext(NotificationContext);
    const graph = useReactFlow();
    const { label } = useMemo(
        () => graph.getNodes().find((n) => n.id === nodeId),
        [nodeId]
    );

    const handleSubmit = useCallback(() => {
        const nodes = graph.getNodes();
        const deletedNode = nodes.find((node) => node.id === nodeId);
        deleteNode(graph, nodeId);
        addNotification(`Node ${deletedNode.data.label} deleted`);
        onSubmit();
    }, [nodeId, graph]);

    return (
        <Modal show onHide={onCancel} backdrop="static" keyboard={false}>
            <Modal.Header closeButton>
                <Modal.Title>Are you sure?</Modal.Title>
            </Modal.Header>
            <Modal.Body className="fs-5">
                You want to delete <span className="fw-bold">{label}</span>?
            </Modal.Body>
            <Modal.Footer>
                <Button variant="outline-secondary flypipe" onClick={onCancel}>
                    no
                </Button>
                <Button variant="outline-danger flypipe" onClick={handleSubmit}>
                    yes
                </Button>
            </Modal.Footer>
        </Modal>
    );
};

export default DeleteNodeModal;
