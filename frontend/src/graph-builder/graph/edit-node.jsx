import React, { useState, useCallback } from "react";
import { Button, Offcanvas, Badge, Row, Col } from "react-bootstrap";
import Form from "react-bootstrap/Form";
import { Tags } from "./tags";
import { NodeMoreInfo } from "./node-more-info";
import { BsInfoLg } from "react-icons/bs";
import CustomSelect from "./CustomSelect";
import { NodeOutput } from "./node-output";
import { NodePredecessors } from "./node-predecessors";
import { NodeSuccessors } from "./node-successors";
import { DeleteNode } from "./delete-node";

export const EditNode = ({ formik, tagsSuggestions, setEditNode }) => {
    const nodeTypeOptions = [
        { value: "", label: "select" },
        { value: "pandas_on_spark", label: "Pandas on Spark" },
        { value: "pyspark", label: "PySpark" },
        { value: "spark_sql", label: "Spark Sql" },
        { value: "pandas", label: "Pandas" },
    ];

    const [show, setShow] = useState(true);
    const [showDeleteNode, setShowDeleteNode] = useState(false);

    const handleClose = () => {
        setShow(false);
        setEditNode(false);
    }

    const handleDeleteNode = () => {
        setShowDeleteNode(true);
    }
    

    const [showMoreInfo, setShowMoreInfo] = useState(false);

    const onClickMoreInfo = useCallback(() => {
        setShowMoreInfo(true);
    }, [formik]);

    return (
        <>
            { showDeleteNode && <DeleteNode id={formik.values.id} label={formik.values.label}  setShowDeleteNode={setShowDeleteNode} setEditNode={setEditNode} /> }
            <NodeMoreInfo
                node={formik.values}
                show={showMoreInfo}
                onHide={handleClose}
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
                        {formik.values.sourceCode && (
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

                    <Form onSubmit={formik.handleSubmit}>
                        <Form.Control
                            type="text"
                            hidden={true}
                            id="id"
                            name="id"
                            defaultValue={formik.values.label}
                        />

                        <Form.Group className="mb-3">
                            <Form.Label className="fw-semibold">
                                Name
                            </Form.Label>
                            <Form.Control
                                type="text"
                                id="label"
                                name="label"
                                value={formik.values.label}
                                onChange={formik.handleChange}
                            />
                            {formik.errors.label ? (
                                <div className="text-danger">
                                    {formik.errors.label}
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
                                value={formik.values.nodeType}
                                onChange={(value) =>
                                    formik.setFieldValue(
                                        "nodeType",
                                        value.value
                                    )
                                }
                            />
                            {formik.errors.nodeType ? (
                                <div className="text-danger">
                                    {formik.errors.nodeType}
                                </div>
                            ) : null}
                        </Form.Group>

                        <Form.Group className="mb-3">
                            <Form.Label className="fw-semibold">
                                Tags
                            </Form.Label>
                            <Tags
                                formik={formik}
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
                                value={formik.values.description}
                                onChange={formik.handleChange}
                                style={{ height: "120px" }}
                            />
                        </Form.Group>

                        <Form.Group className="mb-3">
                            <Form.Label className="fw-semibold">
                                Predecessors
                            </Form.Label>
                            {formik.values.predecessors2 &&
                                Object.keys(formik.values.predecessors2)
                                    .length > 0 && (
                                    <NodePredecessors
                                        dependencies={
                                            formik.values.predecessors2
                                        }
                                    />
                                )}
                            {(!formik.values.predecessors2 ||
                                Object.keys(formik.values.predecessors2)
                                    .length == 0) && <p>No predecessors</p>}
                        </Form.Group>

                        <Form.Group className="mb-3">
                            <Form.Label className="fw-semibold">
                                Successors
                            </Form.Label>
                            {formik.values.successors &&
                                formik.values.successors.length > 0 && (
                                    <NodeSuccessors
                                        successors={formik.values.successors}
                                    />
                                )}
                            {(!formik.values.successors ||
                                formik.values.successors.length == 0) && (
                                <p>No successors</p>
                            )}
                        </Form.Group>

                        <Form.Group className="mb-3">
                            <Form.Label className="fw-semibold">
                                Output Schema
                            </Form.Label>
                            {formik.values.output &&
                                formik.values.output.length > 0 && (
                                    <NodeOutput output={formik.values.output} />
                                )}
                            {(!formik.values.output ||
                                formik.values.output.length == 0) && (
                                <p>No output declared</p>
                            )}
                        </Form.Group>

                        <Row>
                            <Col>
                                <Button variant="outline-danger" onClick={handleDeleteNode}>delete</Button>
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
                                    onClick={handleClose}
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
