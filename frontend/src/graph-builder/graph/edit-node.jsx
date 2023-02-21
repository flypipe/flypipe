import React, { useState, useCallback, useEffect } from "react";
import { Button, Offcanvas, Badge, Row, Col } from "react-bootstrap";
import Form from "react-bootstrap/Form";
import { Tags } from "./tags";
import { NodeMoreInfo } from "./node-more-info";
import { BsInfoLg } from "react-icons/bs";
import CustomSelect from "./CustomSelect";
import { NodeOutput } from "./node-output";
import { NodePredecessors } from "./node-predecessors";
import { NodeSuccessors } from "./node-successors";
import { useFormik } from "formik";


export const EditNode = ({ node, tagsSuggestions }) => {
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
        },
        validate,
        onSubmit: (values) => {
            alert(JSON.stringify(values, null, 2));
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

    const [show, setShow] = useState(true);
    const handleClose = () => setShow(false);

    const [showMoreInfo, setShowMoreInfo] = useState(false);

    const onClickMoreInfo = useCallback(() => {
        setShowMoreInfo(true);
    }, [formState]);

    return (
        <>
            <NodeMoreInfo
                node={formState.values}
                show={showMoreInfo}
                onClose={() => {
                    setShowMoreInfo(false);
                }}
            />

            <Offcanvas
                show={show}
                onHide={handleClose}
                placement="end"
                backdrop={false}
                scroll={true}
                className="node"
            >
                <Offcanvas.Header closeButton={false} className="node">
                    <Offcanvas.Title>
                        Edit Node
                        {formState.values.sourceCode && (
                            <Button
                                variant="outline-secondary flypipe"
                                className="btn-sm float-end"
                                onClick={onClickMoreInfo}
                            >
                                <BsInfoLg />
                            </Button>
                        )}
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
                                formik={formState}
                                tagsSuggestions={tagsSuggestions}
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
                            {formState.values.predecessors2 &&
                                Object.keys(formState.values.predecessors2)
                                    .length > 0 && (
                                    <NodePredecessors
                                        dependencies={
                                            formState.values.predecessors2
                                        }
                                    />
                                )}
                            {(!formState.values.predecessors2 ||
                                Object.keys(formState.values.predecessors2)
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
                                <Button variant="outline-danger">delete</Button>
                                <Button
                                    variant="outline-primary"
                                    className="me-2 float-end"
                                    type="submit"
                                >
                                    save
                                </Button>
                                <Button
                                    variant="outline-secondary flypipe"
                                    className="me-2 float-end"
                                >
                                    close
                                </Button>
                            </Col>
                        </Row>
                    </Form>
                </Offcanvas.Body>
            </Offcanvas>
        </>
    );
};
