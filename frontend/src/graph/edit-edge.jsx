import React, { useState, useCallback, useEffect, useMemo } from "react";
import {
    Button,
    Form,
    Modal,
    Row,
    Col,
    Container,
    Offcanvas,
    Alert,
} from "react-bootstrap";
import { useReactFlow } from "reactflow";
import { deleteEdge } from "../util";
import { DeleteEdge } from "./delete/delete";

export const EditEdge = ({ edge, onClose }) => {
    const isReadOnly = !edge.isNew;
    const [data, setData] = useState({
        columns: [],
        requestedAllColumns: false,
        isUnknownColumns: true,
    });

    const graph = useReactFlow();
    const [sourceNode, targetNode] = useMemo(
        () => [graph.getNode(edge.source), graph.getNode(edge.target)],
        [graph, edge]
    );
    const [formError, setFormError] = useState("");

    useEffect(() => {
        // When an edge is selected, we need to fill out:
        // a) The list of available output columns to select from the source
        // b) The list of currently selected columns by the target
        // Rules:
        // - If the source node has an output schema, we fill out a) from the list of columns here.
        // - If the source node does not have an output schema, we fill out a) from the list of
        // columns requested by successor nodes on all edges where the source is the current node.
        // - If the target node has predecessor columns specified then we use them for b).
        // - If the target node has no predecessor columns specified then we implicitly select all
        // available columns for b).
        const availableNodeIds = new Set(
            graph.getNodes().map((node) => node.id)
        );
        let availableColumns = null;
        let requestedColumns = null;
        let isAvailableColumnsUnknown = null;

        if (sourceNode.data.output.length > 0) {
            availableColumns = sourceNode.data.output.map(
                ({ column }) => column
            );
            isAvailableColumnsUnknown = false;
        } else {
            const successorNodeIds = sourceNode.data.successors;
            availableColumns = Array.from(
                new Set(
                    successorNodeIds
                        .filter(
                            (nodeId) =>
                                availableNodeIds.has(nodeId) &&
                                graph.getNode(nodeId).data.predecessorColumns
                                    .length > 0
                        )
                        .map(
                            (successorNodeId) =>
                                graph.getNode(successorNodeId).data
                                    .predecessorColumns[sourceNode.id]
                        )
                        .reduce(
                            (accumulator, current) => [
                                ...accumulator,
                                ...current,
                            ],
                            []
                        )
                )
            ).sort();
            isAvailableColumnsUnknown = true;
        }

        if (targetNode.data.predecessorColumns[sourceNode.id]) {
            requestedColumns =
                targetNode.data.predecessorColumns[sourceNode.id];
        } else {
            requestedColumns = [];
        }

        // It's not possible to not request any columns from a dependency, we therefore use it as a convenient value
        // to denote that all columns are selected.
        const requestedAllColumns = requestedColumns.length === 0;
        setData({
            columns: availableColumns.map((column) => ({
                column,
                isRequested:
                    requestedAllColumns || requestedColumns.includes(column),
            })),
            requestedAllColumns,
            isUnknownColumns: isAvailableColumnsUnknown,
        });
    }, [graph, sourceNode, targetNode, setData]);

    const handleSelectAll = useCallback(
        (e) => {
            setData((prevData) => ({
                ...prevData,
                columns: prevData.columns.map((column) => ({
                    ...column,
                    isRequested: e.target.checked,
                })),
                requestedAllColumns: e.target.checked,
            }));
        },
        [setData]
    );

    const handleSelectColumn = (e) => {
        const { id: selectedColumn } = e.target;
        const newColumns = data.columns.map(({ column, isRequested }) =>
            column === selectedColumn
                ? { column, isRequested: !isRequested }
                : { column, isRequested }
        );
        const unselectedColumns = newColumns.filter(
            ({ isRequested }) => !isRequested
        );
        let requestedAllColumns = data.requestedAllColumns;
        if (unselectedColumns.length > 0) {
            requestedAllColumns = false;
        } else if (!data.isUnknownColumns) {
            requestedAllColumns = true;
        }
        setData((prevData) => ({
            ...prevData,
            columns: newColumns,
            requestedAllColumns,
        }));
    };

    useEffect(() => {
        const selectedColumns = data.columns.filter(
            ({ isRequested }) => isRequested
        );
        if (data.columns.length > 0 && selectedColumns.length === 0) {
            setFormError("At least one column must be selected");
        } else {
            setFormError("");
        }
    }, [data, setFormError]);

    const handleSubmitDeleteEdge = useCallback(() => {
        handleClose();
    }, [handleClose]);

    const handleClose = useCallback(() => {
        onClose();
    }, [onClose]);

    const handleSave = useCallback(() => {
        const { source: sourceNodeId, target: targetNodeId } = edge;
        const targetNode = graph.getNode(targetNodeId);
        const { columns, requestedAllColumns } = data;
        if (requestedAllColumns) {
            targetNode.data.predecessorColumns[sourceNodeId] = [];
        } else {
            targetNode.data.predecessorColumns[sourceNodeId] = columns
                .filter(({ isRequested }) => isRequested)
                .map(({ column }) => column);
        }
        const otherNodes = graph
            .getNodes()
            .filter(({ id }) => id !== targetNodeId);
        graph.setNodes([...otherNodes, targetNode]);
        onClose();
    }, [edge, data, graph, onClose]);

    return (
        <Offcanvas
            show
            onHide={handleClose}
            placement="end"
            backdrop={false}
            scroll={true}
            className="offcanvas"
        >
            <Offcanvas.Header closeButton={false} className="node">
                <Offcanvas.Title>
                    {isReadOnly ? "View Edge" : "Edit Edge"}
                </Offcanvas.Title>
            </Offcanvas.Header>
            <Offcanvas.Body>
                <h6>Source: {sourceNode.data.label}</h6>
                <h6>Target: {targetNode.data.label}</h6>
                <h6>Requested Columns</h6>
                <Form>
                    <fieldset disabled={isReadOnly}>
                        <Form.Check
                            key="all"
                            type="checkbox"
                            label="all columns"
                            checked={data.requestedAllColumns}
                            onChange={handleSelectAll}
                        />
                        {data.columns.map(({ column, isRequested }) => (
                            <Form.Check
                                key={column}
                                type="checkbox"
                                id={column}
                                label={column}
                                checked={isRequested}
                                onChange={handleSelectColumn}
                            />
                        ))}
                        {data.isUnknownColumns && (
                            <>
                                <Form.Check
                                    key="otherColumns"
                                    type="checkbox"
                                    label="(unknown columns)"
                                    checked={data.requestedAllColumns}
                                    disabled
                                />
                                <br />
                                <Alert variant="warning">
                                    {`Source node ${sourceNode.data.label} does not define an output schema, any available column suggestions are drawn from existing requested columns. There are potentially more columns available to be queried which are denoted by the 'unknown columns' box.`}
                                </Alert>
                            </>
                        )}
                    </fieldset>
                    {formError && <Alert variant="danger">{formError}</Alert>}
                    <Row className="mt-4">
                        <Col>
                            {!isReadOnly && (
                                <DeleteEdge
                                    edgeId={edge.id}
                                    onDelete={handleSubmitDeleteEdge}
                                />
                            )}
                            {!isReadOnly && (
                                <Button
                                    variant="outline-primary"
                                    className="me-2 float-end"
                                    onClick={handleSave}
                                    disabled={formError}
                                >
                                    Save
                                </Button>
                            )}
                            <Button
                                variant="outline-secondary flypipe"
                                className="me-2 float-end"
                                onClick={handleClose}
                            >
                                Close
                            </Button>
                        </Col>
                    </Row>
                </Form>
            </Offcanvas.Body>
        </Offcanvas>
    );
};
