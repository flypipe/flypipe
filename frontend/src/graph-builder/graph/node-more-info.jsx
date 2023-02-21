import React, { useState, useContext } from "react";
import { Modal, Button, Badge } from "react-bootstrap";
import SyntaxHighlighter from "react-syntax-highlighter";
import { CopyToClipboard } from "react-copy-to-clipboard";
import { BsClipboard } from "react-icons/bs";
import { NotificationContext } from "../../context";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import uuid from "react-uuid";

export const NodeMoreInfo = ({ node, show, onClose }) => {
    const [copiedLocation, setCopiedLocation] = useState(false);
    const [copiedPyImport, setCopiedPyImport] = useState(false);
    const [copiedSourceCode, setCopiedSourceCode] = useState(false);
    const { newMessage, setNewMessage } = useContext(NotificationContext);

    const handleClose = () => onClose(false);

    return (
        <Modal
            show={show}
            onHide={handleClose}
            dialogClassName="modal-90w  modal-dialog-scrollable"
        >
            <Modal.Header closeButton>
                <Modal.Title>
                    {node.label}
                    <Badge bg="success" className="ms-2 fs-6 fw-light fw-light">
                        Pandas
                    </Badge>
                    <Badge bg="dark" className="ms-2 fs-6 fw-light fw-light">
                        SKIPPED
                    </Badge>
                </Modal.Title>
            </Modal.Header>
            <Modal.Body>
                <ul className="list-unstyled">
                    <li>
                        Tags:{" "}
                        <Badge bg="light" className="ms-2" text="dark">
                            tag_1
                        </Badge>{" "}
                        <Badge bg="light" className="ms-2" text="dark">
                            tag_2
                        </Badge>
                    </li>
                    <li>
                        Location:
                        { node.filePath && <CopyToClipboard
                            text={node.filePath}
                            className="ms-2 pointer fst-italic text-secondary bg-opacity-10"
                            onCopy={() => {
                                setCopiedLocation(true);
                                setNewMessage({
                                    msgId: uuid(),
                                    message: `Copied ${node.filePath} to clipboard`,
                                });
                            }}
                        >
                            <span>{node.filePath}</span>
                        </CopyToClipboard>
                        }

                        { !node.filePath ? <span className="ms-2 text-secondary">not specified</span> : null}
                    </li>
                    <li>
                        Py Import:
                        { node.pythonImportCommand && <CopyToClipboard
                            text={node.pythonImportCommand}
                            className="ms-2 pointer fst-italic text-secondary bg-opacity-10"
                            onCopy={() => {
                                setCopiedPyImport(true);
                                setNewMessage({
                                    msgId: uuid(),
                                    message: `Copied ${node.pythonImportCommand} to clipboard`,
                                });
                            }}
                        >
                            <span>{node.pythonImportCommand}</span>
                        </CopyToClipboard>
                        }
                        { !node.pythonImportCommand ? <span className="ms-2 text-secondary">not specified</span> : null}
                    </li>
                </ul>

                <div className="position-relative">
                    <CopyToClipboard
                        text={node.sourceCode}
                        className="ms-2"
                        onCopy={() => {
                            setCopiedSourceCode(true);
                            setNewMessage({
                                msgId: uuid(),
                                message: `Copied source code to clipboard`,
                            });
                        }}
                    >
                        <span>
                            <OverlayTrigger
                                key="right"
                                placement="right"
                                overlay={
                                    <Tooltip id="tooltip-right">
                                        copy to clipboard
                                    </Tooltip>
                                }
                            >
                                <Button
                                    variant="light"
                                    className="btn-sm position-absolute ms-n2 mt-2 "
                                >
                                    <BsClipboard />
                                </Button>
                            </OverlayTrigger>
                        </span>
                    </CopyToClipboard>

                    <SyntaxHighlighter
                        language="python"
                        className="border mt-4 p-4 pt-5 shadow-sm rounded h-75"
                    >
                        {node.sourceCode}
                    </SyntaxHighlighter>
                </div>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="outline-secondary" onClick={handleClose}>
                    Close
                </Button>
            </Modal.Footer>
        </Modal>
    );
};
