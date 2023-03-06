import React from "react";
import { Button, Modal } from "react-bootstrap";

const ConfirmDelete = ({ title, description, onCancel, onSubmit }) => {
    return (
        <Modal show onHide={onCancel} backdrop="static" keyboard={false}>
            <Modal.Header closeButton>
                <Modal.Title>{title}</Modal.Title>
            </Modal.Header>
            <Modal.Body className="fs-5">{description}</Modal.Body>
            <Modal.Footer>
                <Button variant="outline-secondary flypipe" onClick={onCancel}>
                    No
                </Button>
                <Button variant="outline-danger flypipe" onClick={onSubmit}>
                    Yes
                </Button>
            </Modal.Footer>
        </Modal>
    );
};

export default ConfirmDelete;
