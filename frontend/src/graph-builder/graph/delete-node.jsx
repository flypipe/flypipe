import React, { useState } from "react";
import { Button, Form, Modal } from "react-bootstrap";
import { useReactFlow } from "reactflow";
import { useFormik } from "formik";

export const DeleteNode = ({ id, label, setShowDeleteNode, setEditNode }) => {
    const [show, setShow] = useState(true);
    const [deleteConfirmed, setDeleteConfirmed] = useState(false);
    
    const graph = useReactFlow();
    
    
    const handleClose = () => {
        setShow(false);
        setShowDeleteNode(false);
    };

    const handleShow = () => setShow(true);


    const formik = useFormik({
        initialValues: {
            id: id
        },
        onSubmit: (values) => {
            console.log("deleted id: ", values);
            // graph.deleteElements([values.id]);
            const nodes = graph.getNodes().filter((n) => n.id !== id)
            graph.setNodes(nodes);
            handleClose();
            setEditNode(false);
        },
    });

    return (
        <>
        
        <Modal show={show} onHide={handleClose} backdrop="static" keyboard={false}>
            <Modal.Header closeButton>
            <Modal.Title>Are you sure?</Modal.Title>
            </Modal.Header>
            <Modal.Body className="fs-5">You want to delete <span className="fw-bold">{label}</span>?</Modal.Body>
            <Modal.Footer>
            <Button variant="outline-secondary flypipe" onClick={handleClose}>
                no
            </Button>
            <Form onSubmit={formik.handleSubmit}>
                <Form.Control
                    type="text"
                    hidden={true}
                    id="id"
                    name="id"
                    defaultValue={formik.values.id}
                />
                <Button type="submit" variant="outline-danger flypipe">
                    yes
                </Button>
            </Form>
            </Modal.Footer>
        </Modal>
        </>
    );
};
