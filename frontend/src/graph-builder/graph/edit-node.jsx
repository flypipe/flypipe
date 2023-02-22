import React, { useState, useCallback, useEffect } from "react";
import { Button, Offcanvas, Badge, Row, Col } from "react-bootstrap";
import Form from "react-bootstrap/Form";
import { Tags } from "./tags";
import { NodeMoreInfo } from "./node-more-info";
import { BsInfoLg } from "react-icons/bs";
import CustomSelect from "./CustomSelect";
import DeleteNodeModal from "./delete-node-modal";
import { NodeOutput } from "./node-output";
import { NodePredecessors } from "./node-predecessors";
import { NodeSuccessors } from "./node-successors";
import { useFormik } from "formik";


export const EditNode = ({ node, tagSuggestions, onClose: handleClose, onSave: handleSave }) => {
    const validate = (values) => {
        const errors = {};

        if (!values.label) {
            errors.label = "Name is required";
        }

        if (!values.nodeType) {
            errors.nodeType = "Type is required";
        }

        return errors;
    };
    const formState = useFormik({
        initialValues: {
            name: "",
            nodeType: "",
            tags: [],
        },
        validate,
        onSubmit: (values) => {
            handleSave(values);
        },
    });
    useEffect(() => {
        formState.resetForm({
            values: {
                id: node.id,
                isNew: node.type === "flypipe-node-new" ? true : false,
                ...node.data,
            },
        });
    }, [node]);

    const nodeTypeOptions = [
        { value: "pandas_on_spark", label: "Pandas on Spark" },
        { value: "pyspark", label: "PySpark" },
        { value: "spark_sql", label: "Spark SQL" },
        { value: "pandas", label: "Pandas" },
    ];

 
    const [showDeleteNode, setShowDeleteNode] = useState(false);

    const handleShowDeleteNode = useCallback(() => {
        setShowDeleteNode(true);
    }, [setShowDeleteNode]);
    const handleCancelDeleteNode = useCallback(() => {
        setShowDeleteNode(false);
    }, [setShowDeleteNode]);
    const handleSubmitDeleteNode = useCallback(() => {
        setShowDeleteNode(false);
        handleClose();
    }, [setShowDeleteNode, handleClose]);

    const [showMoreInfo, setShowMoreInfo] = useState(false);
    const onClickMoreInfo = useCallback(() => {
        setShowMoreInfo(true);
    }, [setShowMoreInfo]);
    const onClickCloseMoreInfo = useCallback(() => {
        setShowMoreInfo(false);
    }, [setShowMoreInfo]);

    const handleSetTags = useCallback((tags) => {
        formState.values.tags = tags.map(tag => tag.id);
        console.log(formState.values.tags);
    }, [formState]);

    return (
        <>
            { showDeleteNode && <DeleteNodeModal nodeId={formState.values.id} onCancel={handleCancelDeleteNode} onSubmit={handleSubmitDeleteNode} handleClose={handleCancelDeleteNode} /> }
            <NodeMoreInfo
                node={formState.values}
                show={showMoreInfo}
                onClose={onClickCloseMoreInfo}
            />

            <Offcanvas
                show={show}
                // onHide={handleClose}
                placement="end"
                backdrop={false}
                scroll={true}
                className="node"
            >
                <Offcanvas.Header closeButton={false} className="node">
                    <Offcanvas.Title>
                        Edit Node
                        <Button
                            variant="outline-secondary flypipe"
                            className="btn-sm float-end"
                            onClick={onClickMoreInfo}
                        >
                            <BsInfoLg />
                        </Button>
                    </Offcanvas.Title>
                </Offcanvas.Header>
                <Offcanvas.Body>
                    <p>
                        <span className="fw-semibold">Status</span>
                        <Badge
                            bg="success float-end"
                            className="text-uppercase"
                        >
                            active
                        </Badge>
                    </p>

                    <Form onSubmit={formState.handleSubmit}>
                        <Form.Control
                            type="text"
                            hidden={true}
                            id="id"
                            name="id"
                            defaultValue={formState.values.label}
                        />

                        <Form.Group className="mb-3">
                            <Form.Label className="fw-semibold">
                                Name
                            </Form.Label>
                            <Form.Control
                                type="text"
                                id="label"
                                name="label"
                                value={formState.values.label}
                                onChange={formState.handleChange}
                            />
                            {formState.errors.label ? (
                                <div className="text-danger">
                                    {formState.errors.label}
                                </div>
                            ) : null}
                        </Form.Group>

                        <Form.Group className="mb-3">
                            <Form.Label className="fw-semibold">
                                Type
                            </Form.Label>
                            <CustomSelect
                                id="nodeType"
                                name="nodeType"
                                options={nodeTypeOptions}
                                value={formState.values.nodeType}
                                onChange={(value) =>
                                    formState.setFieldValue(
                                        "nodeType",
                                        value.value
                                    )
                                }
                            />
                            {formState.errors.nodeType ? (
                                <div className="text-danger">
                                    {formState.errors.nodeType}
                                </div>
                            ) : null}
                        </Form.Group>

                        <Form.Group className="mb-3">
                            <Form.Label className="fw-semibold">
                                Tags
                            </Form.Label>
                            <Tags
                                tags={formState.values.tags.map(tag => ({id: tag, text: tag}))}
                                tagSuggestions={tagSuggestions}
                                onSetTags={handleSetTags}
                            />
                        </Form.Group>

                        <Form.Group className="mb-3">
                            <Form.Label className="fw-semibold">
                                Description
                            </Form.Label>
                            <Form.Control
                                as="textarea"
                                id="description"
                                name="description"
                                value={formState.values.description}
                                onChange={formState.handleChange}
                                style={{ height: "120px" }}
                            />
                        </Form.Group>

                        <Form.Group className="mb-3">
                            <Form.Label className="fw-semibold">
                                Predecessors
                            </Form.Label>
                            {formState.values.predecessors_columns &&
                                Object.keys(formState.values.predecessors_columns)
                                    .length > 0 && (
                                    <NodePredecessors
                                        dependencies={
                                            formState.values.predecessors_columns
                                        }
                                    />
                                )}
                            {(!formState.values.predecessors_columns ||
                                Object.keys(formState.values.predecessors_columns)
                                    .length == 0) && <p>No predecessors</p>}
                        </Form.Group>

                        <Form.Group className="mb-3">
                            <Form.Label className="fw-semibold">
                                Successors
                            </Form.Label>
                            {formState.values.successors &&
                                formState.values.successors.length > 0 && (
                                    <NodeSuccessors
                                        successors={formState.values.successors}
                                    />
                                )}
                            {(!formState.values.successors ||
                                formState.values.successors.length == 0) && (
                                <p>No successors</p>
                            )}
                        </Form.Group>

                        <Form.Group className="mb-3">
                            <Form.Label className="fw-semibold">
                                Output Schema
                            </Form.Label>
                            {formState.values.output &&
                                formState.values.output.length > 0 && (
                                    <NodeOutput output={formState.values.output} />
                                )}
                            {(!formState.values.output ||
                                formState.values.output.length == 0) && (
                                <p>No output declared</p>
                            )}
                        </Form.Group>

                        <Row>
                            <Col>
                                <Button variant="outline-danger" onClick={handleShowDeleteNode}>Delete</Button>
                                <Button
                                    variant="outline-primary"
                                    className="me-2 float-end"
                                    type="submit"
                                >
                                    Save
                                </Button>
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
        </>
    );
};
