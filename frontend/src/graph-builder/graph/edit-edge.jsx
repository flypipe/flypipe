import React, { useState, useCallback, useEffect } from "react";
import { Button, Form, Modal, Row, Col, Container, Offcanvas } from "react-bootstrap";
import { useReactFlow } from "reactflow";
import { useFormik } from "formik";
import { uuid } from  "react-uuid";

export const EditEdge = ({ edge, setShowEditEdge }) => {

    const [allChecked, setAllChecked] = useState(false);
    const [listChecked, setListChecked] = useState([]);
    const [sourceOutputColumns, setSourceOutputColumns] = useState([]);
    const [supersetColumns, setSupersetColumns] = useState([]);

    const graph = useReactFlow();
    const nodeSource = graph.getNode(edge.source);
    const nodeTarget = graph.getNode(edge.target);
    
    console.log("Node source:", nodeSource);
    console.log("Node target:", nodeTarget);


    useEffect(() => {

        const supersetColumns = [];
        const outputColumns = [];
        nodeSource.data.output.forEach((output) => {
            outputColumns.push(output.column);
            supersetColumns.push(output.column);
        });

        if (nodeSource.data.nodeKey.has(nodeSource.data.nodeKey)){
            nodeTarget.data.predecessor_columns[nodeSource.data.nodeKey].forEach((column) => {
                if (!supersetColumns.includes(column)){
                    supersetColumns.push(column);    
                }            
            });
        }

        setSupersetColumns(supersetColumns);
        setSourceOutputColumns(outputColumns);
        setListChecked(nodeTarget.data.predecessor_columns[nodeSource.data.nodeKey]); 

    }, [nodeSource, nodeTarget]);
    
    console.log("sourceOutputColumns: ",sourceOutputColumns);
    console.log("supersetColumns: ",supersetColumns);
    console.log("listChecked: ",listChecked);
    
    const handleSelectAll = useCallback((e) => {
        if(allChecked){
            setListChecked([]);
        }
        else{
            setListChecked(supersetColumns);
        }

        setAllChecked(!allChecked);        
    });

    const handleCheck = (e) => {
        const { id, _ } = e.target;
        if (listChecked.includes(id)) {
            setListChecked(prev => prev.filter(item => item !== id));
            setAllChecked(false);
        }
        else{
            setListChecked([...listChecked, id]);
        }
    }

    const handleClose = () => {
        setShowEditEdge(false);
    };

    const formik = useFormik({
        initialValues: {
            id: "edge.id"
        },
        onSubmit: (values) => {
            handleClose();
        },
    });

    
    const dependencies = [];
    supersetColumns.forEach((column) => {
        var foundInSourceOutput = "";
        if (!sourceOutputColumns.includes(column)){
            foundInSourceOutput = `(not found in ${nodeSource.data.label} output)`
        }
        
        dependencies.push(
            <Form.Check 
                key={column}
                type="checkbox"
                label={`${column} ${foundInSourceOutput}`}
                id={column}
                name={column}
                checked={listChecked.includes(column)}
                className="ms-2"
                onChange={handleCheck}
            />
        );
    });
    

    

    return (
        <>
        <Offcanvas
                show
                onHide={handleClose}
                placement="end"
                backdrop={false}
                scroll={true}
                className="node"
            >
                <Offcanvas.Header closeButton={false} className="node">
                    <Offcanvas.Title>Edit Edge</Offcanvas.Title>
                </Offcanvas.Header>
                <Offcanvas.Body>
                    <Form onSubmit={formik.handleSubmit}>

                        <Form.Check 
                            key="all"
                            type="checkbox"
                            label="Select all"
                            checked={allChecked}
                            onChange={handleSelectAll}
                        />
                        {dependencies}
                        
                        <Row className="mt-4">                                                  
                            <Col>                                
                                <Button variant="outline-danger">delete</Button>
                                <Button variant="outline-primary" className="me-2 float-end" type="submit">save</Button>
                                <Button variant="outline-secondary flypipe" className="me-2 float-end" onClick={handleClose}>close</Button>                                    
                            </Col>                                                    
                        </Row>                        
                    </Form>
                </Offcanvas.Body>
            </Offcanvas>

        
        </>
    );
};
