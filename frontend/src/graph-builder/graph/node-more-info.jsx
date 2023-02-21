import React, { useState, useContext } from 'react';
import {Modal, Button, Badge} from 'react-bootstrap';
import SyntaxHighlighter from 'react-syntax-highlighter';
import {CopyToClipboard} from 'react-copy-to-clipboard';
import { HiOutlineClipboardDocumentCheck } from "react-icons/hi2";
import { NotificationContext } from '../../context';
import uuid from 'react-uuid';

export const NodeMoreInfo = ( { node, show, onClose }) => {   
    const [copiedLocation, setCopiedLocation] = useState(false);
    const [copiedPyImport, setCopiedPyImport] = useState(false);
    const { newMessage, setNewMessage } = useContext(NotificationContext);
    
    const handleClose = () => onClose(false);

    return (
        <Modal show={show} onHide={handleClose} dialogClassName="modal-90w  modal-dialog-scrollable">
        <Modal.Header closeButton>
          <Modal.Title>
            {node.label}
            <Badge bg="success" className='ms-2 fs-6 fw-light fw-light'>Pandas</Badge>
            <Badge bg="dark" className='ms-2 fs-6 fw-light fw-light'>SKIPPED</Badge>
          </Modal.Title>
        </Modal.Header>
        <Modal.Body>
          <ul className='list-unstyled'>
            
            <li>Tags: <Badge bg="light" className='ms-2' text="dark">tag_1</Badge> <Badge bg="light" className='ms-2' text="dark">tag_2</Badge></li>
            <li>Location: <span  className="fst-italic text-secondary bg-opacity-10">/this/is/a/location/country_code_1.py</span></li>
            <li>
            Location: 
              <CopyToClipboard text="/this/is/a/location/country_code_1.py" onCopy={() => {
                setCopiedLocation(true);
                console.log("setting message")
                setNewMessage({
                  msgId: uuid(),
                  message: "Copied /this/is/a/location/country_code_1.py to clipboard"
              });
              }}>
                <span  className="fst-italic text-secondary bg-opacity-10">/this/is/a/location/country_code_1.py</span>
              </CopyToClipboard>
              
            </li>
            <li>
              Py Import: 
              <CopyToClipboard text="from xyz.country_code_1 import country_code_1" onCopy={() => setCopiedPyImport(true)}>
                <span  className="fst-italic text-secondary bg-opacity-10">from xyz.country_code_1 import country_code_1</span>
              </CopyToClipboard>
              {copiedPyImport ? <span><HiOutlineClipboardDocumentCheck/> copied</span> : null}
            </li>
          </ul>
          
          <SyntaxHighlighter
              language="python"
              className="border mt-4 p-4 shadow-sm rounded h-75"
          >
              {node.sourceCode}
              
          </SyntaxHighlighter>

        </Modal.Body>
        <Modal.Footer>
          <Button variant="outline-secondary" onClick={handleClose}>
            Close
          </Button>          
        </Modal.Footer>
      </Modal>
    )

};