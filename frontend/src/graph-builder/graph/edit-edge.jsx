import React, { useState, useCallback } from "react";
import { Button, Form, Modal, Row, Col, Container, Offcanvas } from "react-bootstrap";
import { useReactFlow } from "reactflow";
import { useFormik } from "formik";
import { uuid } from  "react-uuid";

export const EditEdge = ({ edge, setShowEditEdge }) => {

    const [allChecked, setAllChecked] = useState(false);
    

    const graph = useReactFlow();    
    const nodeSource = graph.getNode(edge.source);
    const nodeTarget = graph.getNode(edge.target);

    const handleSelectAll = useCallback((e) => {
        if(allChecked){
            setListChecked([]);
        }
        else{
            setListChecked(superSetColumns);
        }

        setAllChecked(!allChecked);        
    });

    const handleCheck = (e) => {
        const { id, _ } = e.target;
        const isChecked = listChecked.includes(id);
        console.log("1===>",id, isChecked, listChecked);
        if (isChecked) {
            setListChecked(prev => prev.filter(item => item !== id));
        }
        else{
            setListChecked([...listChecked, id]);
        }
        console.log("3===>",id, isChecked, listChecked);
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

    const dependentColumns = nodeSource.id in nodeTarget.data.predecessor_columns? nodeTarget.data.predecessor_columns[nodeSource.id] : [];
    const [listChecked, setListChecked] = useState(dependentColumns);
    console.log("listChecked:",listChecked, dependentColumns)

    const outputColumns = [];
    nodeSource.data.output.forEach((output) => {
        outputColumns.push(output.column);
    });

    const superSetColumns = [];
    dependentColumns.forEach((dependentColumn) => {
        superSetColumns.push(dependentColumn);
    });

    outputColumns.forEach((outputColumn) => {
        if (!superSetColumns.includes(outputColumn)){
            superSetColumns.push(outputColumn);
        }
    });
    console.log(superSetColumns.length, dependentColumns.length);
    

    superSetColumns.forEach((column) => {
        var foundInSourceOutput = "";
        if (!outputColumns.includes(column)){
            foundInSourceOutput = `(not found in ${nodeSource.data.label} output)`
        }
        
        console.log("listChecked.includes(column):", listChecked, column, listChecked.includes(column))
        if (dependentColumns.includes(column) && !listChecked.includes(column)){
            console.log("listChecked=> ", dependentColumns, column, [...listChecked, column])
            setListChecked([...listChecked, column]);
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
                        {/* <Form.Control type="text" hidden={true} id="id" name="id" defaultValue={formik.values.id}/>  */}

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
