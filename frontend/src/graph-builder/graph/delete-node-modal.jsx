import React, { useMemo, useCallback, useContext } from "react";
import { Button, Form, Modal } from "react-bootstrap";
import { useReactFlow } from "reactflow";
import { NotificationContext } from "../../context";
import { deleteNode } from "../util";

const DeleteNodeModal = ({ nodeId, onCancel, onSubmit }) => {
    const { addNotification } = useContext(NotificationContext);
    const graph = useReactFlow();
    const { label } = useMemo(
        () => graph.getNodes().find((n) => n.id === nodeId).data,
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
                <Modal.Title>Delete Node</Modal.Title>
            </Modal.Header>
            <Modal.Body className="fs-5">
                Are you sure you want to delete{" "}
                <span className="fw-bold">{label}</span>?
            </Modal.Body>
            <Modal.Footer>
                <Button variant="outline-secondary flypipe" onClick={onCancel}>
                    No
                </Button>
                <Button variant="outline-danger flypipe" onClick={handleSubmit}>
                    Yes
                </Button>
            </Modal.Footer>
        </Modal>
    );
};

export default DeleteNodeModal;
