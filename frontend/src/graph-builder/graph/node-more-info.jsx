import React, { useCallback, useContext, useMemo } from "react";
import { Modal, Button, Badge } from "react-bootstrap";
import SyntaxHighlighter from "react-syntax-highlighter";
import { NotificationContext } from "../../context";
import uuid from "react-uuid";
import CopyToClipboardWidget from "../../copy-to-clipboard-widget";
import { generateCodeTemplate } from "../util";
import { useReactFlow } from "reactflow";

// Beware that nodeData is from form state not a state variable, this form state does not change object reference when
// it's changed so any usage of nodeData for memoisation must be on individual attributes.
export const NodeMoreInfo = ({ nodeData, show, onClose: handleClose }) => {
    const graph = useReactFlow();
    const { newMessage, setNewMessage } = useContext(NotificationContext);

    const handleCopy = useCallback(
        (data) => {
            // We have limited screen real estate in a toast message so if the copied data is too long don't show it
            const message =
                data.length > 40
                    ? "Copied to clipboard"
                    : `Copied ${data} to clipboard`;
            setNewMessage({
                msgId: uuid(),
                message,
            });
        },
        [setNewMessage]
    );

    const sourceCode = useMemo(
        () =>
            nodeData.sourceCode
                ? nodeData.sourceCode
                : generateCodeTemplate(graph, nodeData),
        [nodeData.tags]
    );

    return (
        <Modal
            show={show}
            onHide={handleClose}
            dialogClassName="modal-more-info modal-dialog-scrollable"
        >
            <Modal.Header closeButton>
                <Modal.Title>
                    {nodeData.label}
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
                    <span>Tags: </span>
                    <Badge bg="light" className="ms-2" text="dark">
                        tag_1
                    </Badge>{" "}
                    <Badge bg="light" className="ms-2" text="dark">
                        tag_2
                    </Badge>
                </div>
                <div>
                    <span>Location:</span>
                    {nodeData.filePath ? (
                        <>
                            <CopyToClipboardWidget
                                text={nodeData.filePath}
                                data={nodeData.filePath}
                                onCopy={handleCopy}
                            >
                                <span>{nodeData.filePath}</span>
                            </CopyToClipboardWidget>
                        </>
                    ) : (
                        <span className="ms-2 text-secondary">N/A</span>
                    )}
                </div>
                <div>
                    <span>Py Import:</span>
                    {nodeData.pythonImportCommand ? (
                        <CopyToClipboardWidget
                            text={nodeData.pythonImportCommand}
                            data={nodeData.pythonImportCommand}
                            onCopy={handleCopy}
                        >
                            <span>{nodeData.pythonImportCommand}</span>
                        </CopyToClipboardWidget>
                    ) : (
                        <span className="ms-2 text-secondary">N/A</span>
                    )}
                </div>

                <div className="position-relative">
                    <CopyToClipboardWidget
                        data={sourceCode}
                        className="mt-2"
                        onCopy={handleCopy}
                    />
                    <SyntaxHighlighter
                        language="python"
                        className="border mt-4 p-4 pt-5 shadow-sm rounded h-75"
                    >
                        {sourceCode}
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
