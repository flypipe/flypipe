import React from 'react';
import { Modal, Button, Tooltip, OverlayTrigger, Offcanvas  } from 'react-bootstrap';
import Form from 'react-bootstrap/Form';
import { AiOutlineDelete } from 'react-icons/ai';

export const EditNode = () => {

    return (
        <Modal show='true' size="lg">
            <Modal.Header closeButton>
            <Modal.Title>
                Edit Node 
                <OverlayTrigger key='right' placement='right'
                    
                    overlay={
                        <Tooltip id="tooltip-node-id">
                        Delete node
                        </Tooltip>
                    }
                >
                    <Button variant="outline-danger" className="m-2"><AiOutlineDelete /></Button> 
                </OverlayTrigger>
                
            </Modal.Title>
            </Modal.Header>
            <Modal.Body>
                <Form.Group className="mb-3">
                    <Form.Label>Name</Form.Label>
                    <Form.Control type="text"/>
                </Form.Group>

                <Form.Group className="mb-3">
                    <Form.Label>Type</Form.Label>
                    <Form.Select>
                        <option>Select</option>
                        <option value="pandas_on_spark">Pandas on Spark</option>
                        <option value="pyspark">PySpark</option>
                        <option value="pandas">Pandas</option>
                        <option value="spark_sql">Spark SQL</option>
                    </Form.Select>            
                </Form.Group>

                <Form.Group className="mb-3">
                    <Form.Label>Tags</Form.Label>
                    <Form.Control type="text" placeholder="tag1, tag2, tag3"/>
                    <Form.Text className="text-muted">
                        List of tags separated by comma.
                    </Form.Text>
                </Form.Group>

                <Form.Group className="mb-3">
                    <Form.Label>Description</Form.Label>
                    <Form.Control as="textarea" style={{ height: '120px' }}/>
                </Form.Group>

                <Form.Group className="mb-3">
                    <Form.Label>Dependencies</Form.Label>
                    <Form.Check type="switch" id="custom-switch" label="Dependency 1"/>
                    <Form.Check type="switch" id="custom-switch" label="col 1" className="m-4"/>
                    <Form.Check type="switch" id="custom-switch" label="col 2" className="m-4"/>
                    <Form.Check type="switch" id="custom-switch" label="col 2" className="m-4"/>
                    <Form.Check type="switch" id="custom-switch" label="col 2" className="m-4"/>
                    
                    
                </Form.Group>

            </Modal.Body>
            <Modal.Footer>
                <Button variant="outline-secondary">Close</Button>
                <Button variant="outline-primary">Save</Button>
            </Modal.Footer>
        </Modal>
    );

}