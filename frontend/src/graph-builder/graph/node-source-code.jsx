

import React, { useState } from 'react';
import {Modal, Button} from 'react-bootstrap';

export const NodeSourceCode = ( { sourceCode, show, onClose }) => {   
    

    const handleClose = () => onClose();

    return (
        <Modal show={show} onHide={handleClose}>
        <Modal.Header closeButton>
          <Modal.Title>Source Code</Modal.Title>
        </Modal.Header>
        <Modal.Body>{sourceCode}</Modal.Body>
        <Modal.Footer>
          <Button variant="outline-secondary" onClick={handleClose}>
            Close
          </Button>          
        </Modal.Footer>
      </Modal>
    )

};