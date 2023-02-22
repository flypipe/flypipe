import React, { useCallback, useContext } from "react";
import { Modal, Button, Badge } from "react-bootstrap";
import SyntaxHighlighter from "react-syntax-highlighter";
import { NotificationContext } from "../../context";
import uuid from "react-uuid";
import CopyToClipboardWidget from "../../copy-to-clipboard-widget";

export const NodeMoreInfo = ({ node, show, onClose: handleClose }) => {
    const { newMessage, setNewMessage } = useContext(NotificationContext);

    const handleCopy = useCallback((data) => {
        // We have limited screen real estate in a toast message so if the copied data is too long don't show it
        const message = (data.length > 40) ? 'Copied to clipboard' : `Copied ${data} to clipboard`;
        setNewMessage({
            msgId: uuid(),
            message,
        });
    }, [setNewMessage]);

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
                <div>
                    <span>Tags:{" "}</span>
                    <Badge bg="light" className="ms-2" text="dark">
                        tag_1
                    </Badge>{" "}
                    <Badge bg="light" className="ms-2" text="dark">
                        tag_2
                    </Badge>
                </div>
                <div>
                    <span>Location:</span>
                    { 
                        node.filePath ? 
                            <>
                                <CopyToClipboardWidget
                                    text={node.filePath}
                                    data={node.filePath}
                                    onCopy={handleCopy}
                                >
                                    <span>{node.filePath}</span>
                                </CopyToClipboardWidget>
                            </>
                        : <span className="ms-2 text-secondary">N/A</span>
                    }
                </div>
                <div>
                    <span>Py Import:</span>
                    { 
                        node.pythonImportCommand ? 
                            <CopyToClipboardWidget
                                text={node.pythonImportCommand}
                                data={node.pythonImportCommand}
                                onCopy={handleCopy}
                            >
                                <span>{node.pythonImportCommand}</span>
                            </CopyToClipboardWidget>
                        : <span className="ms-2 text-secondary">N/A</span>
                    }
                </div>

                <div className="position-relative">
                    <CopyToClipboardWidget
                        data={node.sourceCode}
                        className="mt-2"
                        onCopy={handleCopy}
                    >
                        <span>{node.sourceCode}</span>
                    </CopyToClipboardWidget>

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
