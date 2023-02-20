import React, { useState, useCallback } from 'react';
import { Button, Offcanvas, Badge, Row, Col, Accordion, Modal} from 'react-bootstrap';
import Form from 'react-bootstrap/Form';
import {Tags} from './tags';
import {NodeSourceCode} from './node-source-code';
import { BsCodeSlash } from "react-icons/bs";
import { useReactFlow } from 'reactflow';
import CustomSelect from './CustomSelect';



export const EditNode = ({ formik, tagsSuggestions }) => {   
    const nodeTypeOptions = [
        {value: '', label: 'select'},
        {value: 'pandas_on_spark', label: 'Pandas on Spark'},
        {value: 'pyspark', label: 'PySpark'},
        {value: 'spark_sql', label: 'Spark Sql'},
        {value: 'pandas', label: 'Pandas'}        
    ]
    
    const [show, setShow] = useState(true);
    const handleClose = () => setShow(false);
    
    const [showSourceCode, setShowSourceCode] = useState(false);
    const [sourceCode, setSourcecode] = useState(null);


    const onClickSourceCode = useCallback(() => {
        setSourcecode(formik.values.sourceCode);
        setShowSourceCode(true);
    }, [formik]);

    return (
        <>
        <NodeSourceCode sourceCode={sourceCode}  show={showSourceCode} onClose={() => {setShowSourceCode(false)}} />

        <Offcanvas show={show} onHide={handleClose} placement='end' backdrop={false} scroll={true} className='node'>
            <Offcanvas.Header closeButton={false} className='node'>
                <Offcanvas.Title>
                    Edit Node
                    { formik.values.sourceCode && <Button variant="outline-dark" className="btn-sm float-end" onClick={onClickSourceCode}><BsCodeSlash /></Button> }
                </Offcanvas.Title>                
            </Offcanvas.Header>
            <Offcanvas.Body>
                <p><span className='fw-semibold'>Status</span><Badge bg="success float-end" className="text-uppercase">active</Badge></p>
                

                <Form onSubmit={formik.handleSubmit}>
                    <Form.Group className="mb-3">
                        <Form.Label className="fw-semibold">Name</Form.Label>
                        <Form.Control type="text" id="label" name="label" value={formik.values.label} onChange={formik.handleChange}/>
                        {formik.errors.label ? <div className='text-danger'>{formik.errors.label}</div>: null}
                    </Form.Group>

                    <Form.Group className="mb-3">
                        <Form.Label className="fw-semibold">Type</Form.Label>
                        <CustomSelect 
                          id="nodeType" 
                          name="nodeType"
                          options={nodeTypeOptions}
                          value={formik.values.nodeType}
                          onChange={value=>formik.setFieldValue('nodeType', value.value)}
                        />            
                        {formik.errors.nodeType ? <div className='text-danger'>{formik.errors.nodeType}</div>: null}
                    </Form.Group>

                    <Form.Group className="mb-3">
                        <Form.Label className="fw-semibold">Tags</Form.Label>
                        <Tags formik={formik}/>                        
                    </Form.Group>

                    <Form.Group className="mb-3">
                        <Form.Label className="fw-semibold">Description</Form.Label>
                        <Form.Control as="textarea"  id="description" name="description" value={formik.values.description} onChange={formik.handleChange} style={{ height: '120px' }}/>
                    </Form.Group>

                    <Form.Group className="mb-3">
                        <Form.Label className="fw-semibold">Dependencies</Form.Label>
                        <Accordion>
                            <Accordion.Item eventKey="Dependency 1">
                                <Accordion.Header>Dependency 1</Accordion.Header>
                                <Accordion.Body>
                                    <Form.Check type="checkbox" id="custom-switch" label="ALL" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                </Accordion.Body>
                                
                            </Accordion.Item>
                            <Accordion.Item eventKey="Dependency 2">
                                <Accordion.Header>Dependency 2</Accordion.Header>
                                <Accordion.Body>
                                    <Form.Check type="checkbox" id="custom-switch" label="ALL" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                    <Form.Check type="checkbox" id="custom-switch" label="col 1" className="m-4"/>
                                </Accordion.Body>
                            </Accordion.Item>                            
                        </Accordion>
                    </Form.Group>

                    <Form.Group className="mb-3">
                        <Form.Label className="fw-semibold">Output Schema</Form.Label>   
                        <Accordion>
                            <Accordion.Item eventKey="col1">
                                <Accordion.Header>
                                    <div className="d-flex">
                                        <div className="mr-auto p-2">col1</div>
                                        <div className="p-2"><Badge bg="secondary">String</Badge></div>
                                    </div> 
                                </Accordion.Header>
                                <Accordion.Body>
                                    It is a long established fact that a reader will be distracted by the readable content of a page when looking at its layout. The point of using Lorem Ipsum is that it has a more-or-less normal distribution of letters, as opposed to using 'Content here, content here', making it look like readable English. Many desktop publishing packages and web page editors now use Lorem Ipsum as their default model text, and a search for 'lorem ipsum' will uncover many web sites still in their infancy. Various versions have evolved over the years, sometimes by accident, sometimes on purpose (injected humour and the like).
                                </Accordion.Body>
                            </Accordion.Item>
                            <Accordion.Item eventKey="col2">
                                <Accordion.Header>
                                    <div className="d-flex">
                                        <div className="mr-auto p-2">col2</div>
                                        <div className="p-2"><Badge bg="secondary">Integer</Badge></div>
                                    </div> 
                                </Accordion.Header>
                                <Accordion.Body>
                                    It is a long established fact that a reader will be distracted by the readable content of a page when looking at its layout. The point of using Lorem Ipsum is that it has a more-or-less normal distribution of letters, as opposed to using 'Content here, content here', making it look like readable English. Many desktop publishing packages and web page editors now use Lorem Ipsum as their default model text, and a search for 'lorem ipsum' will uncover many web sites still in their infancy. Various versions have evolved over the years, sometimes by accident, sometimes on purpose (injected humour and the like).
                                </Accordion.Body>
                            </Accordion.Item>
                        </Accordion>                        
                    </Form.Group>
                    <Row>
                        <Col>
                            <Button variant="outline-danger">delete</Button>
                            <Button variant="outline-primary" className="me-2 float-end" type="submit">save</Button>
                            <Button variant="outline-secondary" className="me-2 float-end">close</Button>
                        </Col>
                    </Row>
                </Form>
                
                
                

            </Offcanvas.Body>
        </Offcanvas>
        
        </>
    );

}