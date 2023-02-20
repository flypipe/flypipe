import React from 'react';
import { Badge, Accordion } from 'react-bootstrap';
import Form from 'react-bootstrap/Form';

export const NodePredecessors = ({ dependencies }) => {   
    console.log("dependencies=>", dependencies);
    const description = []

    dependencies.forEach((dependency) => {
        
        const columns = []

        dependency.columns.forEach((column) => {
            columns.push(<Form.Check type="checkbox" id="custom-switch" label={column} className="m-4" key={`${dependency.nodeKey}_${column}`}/>);
        })

        description.push(
            <Accordion.Item eventKey={dependency.nodeKey}  key={dependency.nodeKey}>
                <Accordion.Header>{dependency.nodeKey}</Accordion.Header>
                <Accordion.Body>
                    {columns}                    
                </Accordion.Body>                
            </Accordion.Item>


           
         )
    })

    return (<Accordion>{description}</Accordion>);
}