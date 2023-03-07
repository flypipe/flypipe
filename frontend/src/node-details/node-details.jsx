import React, { useCallback, useContext, useMemo } from "react";
import { Modal, Button, Badge } from "react-bootstrap";
import SyntaxHighlighter from "react-syntax-highlighter";
import { NotificationContext } from "../notifications/context";
import uuid from "react-uuid";
import CopyToClipboardWidget from "../copy-to-clipboard-widget";
import { generateCodeTemplate, getNodeTypeColorClass } from "../util";
import { useReactFlow } from "reactflow";
import { NodeDetailsContext } from "./context";

// Beware that nodeData is from form state not a state variable, this form state does not change object reference when
// it's changed so any usage of nodeData for memoisation must be on individual attributes.
export const NodeDetails = () => {
    const { nodeDetailsState, setNodeDetailsState } =
        useContext(NodeDetailsContext);
    const { newMessage, setNewMessage } = useContext(NotificationContext);
    const graph = useReactFlow();
    const { nodeData, visible } = nodeDetailsState;

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

    const sourceCode =
        nodeData.sourceCode || generateCodeTemplate(graph, nodeData);

    const nodeTypeColorClass = getNodeTypeColorClass(nodeData.nodeType);
    const handleClose = useCallback(() => {
        setNodeDetailsState((prevState) => ({ ...prevState, visible: false }));
    }, [setNodeDetailsState]);

    return (
        <Modal
            show={visible}
            onHide={handleClose}
            dialogClassName="modal-more-info modal-dialog-scrollable"
        >
            <Modal.Header closeButton>
                <Modal.Title className="d-flex justify-content-between">
                    {nodeData.name}
                </Modal.Title>
            </Modal.Header>
            <Modal.Body>
                <div>
                    <Badge bg={nodeTypeColorClass} className="fw-light">
                        {nodeData.nodeType}
                    </Badge>
                    <Badge bg="dark" className="ms-2 fw-light">
                        {nodeData.isActive ? "ACTIVE" : "SKIPPED"}
                    </Badge>
                </div>
                <div>
                    <span>Tags: </span>
                    {nodeData.tags.length > 0 ? (
                        nodeData.tags.map(({ id, text }) => (
                            <Badge
                                key={id}
                                bg="light"
                                className="ms-2"
                                text="dark"
                            >
                                {text}
                            </Badge>
                        ))
                    ) : (
                        <span className="ms-2 text-secondary">N/A</span>
                    )}
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
                    {nodeData.importCmd ? (
                        <CopyToClipboardWidget
                            text={nodeData.importCmd}
                            data={nodeData.importCmd}
                            onCopy={handleCopy}
                        >
                            <span>{nodeData.importCmd}</span>
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
