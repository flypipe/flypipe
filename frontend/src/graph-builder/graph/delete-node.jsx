import React, { useState } from "react";
import { Button, Form, Modal } from "react-bootstrap";
import { useReactFlow } from "reactflow";
import { useFormik } from "formik";

export const DeleteNode = ({
    nodeKey,
    label,
    setShowDeleteNode,
    setEditNode,
}) => {
    const [deleteConfirmed, setDeleteConfirmed] = useState(false);

    const graph = useReactFlow();

    const handleClose = () => {
        setShowDeleteNode(false);
    };

    const formik = useFormik({
        initialValues: {
            nodeKey,
        },
        onSubmit: (values) => {
            const nodes = graph.getNodes().filter((n) => n.nodeKey !== nodeKey);
            graph.setNodes(nodes);
            handleClose();
            setEditNode(false);
        },
    });

    return (
        <>
            <Modal show onHide={handleClose} backdrop="static" keyboard={false}>
                <Modal.Header closeButton>
                    <Modal.Title>Delete Node</Modal.Title>
                </Modal.Header>
                <Modal.Body className="fs-5">
                    Are you sure you want to delete{" "}
                    <span className="fw-bold">{label}</span>?
                </Modal.Body>
                <Modal.Footer>
                    <Button
                        variant="outline-secondary flypipe"
                        onClick={handleClose}
                    >
                        No
                    </Button>
                    <Form onSubmit={formik.handleSubmit}>
                        <Form.Control
                            type="text"
                            hidden={true}
                            id="id"
                            name="id"
                            defaultValue={formik.values.nodeKey}
                        />
                        <Button type="submit" variant="outline-danger flypipe">
                            Yes
                        </Button>
                    </Form>
                </Modal.Footer>
            </Modal>
        </>
    );
};
