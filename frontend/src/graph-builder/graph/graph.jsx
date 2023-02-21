import React, { useCallback, useRef, useState } from "react";
import Search from "../search/search";
import ReactFlow, {
    useReactFlow,
    Controls,
    Background,
    Panel,
    MiniMap,
} from "reactflow";
import { BsDownload } from "react-icons/bs";
import { ExistingNode, NewNode } from "./node";
import { refreshNodePositions, moveToNode } from "../util";
import "reactflow/dist/style.css";
import { MIN_ZOOM, MAX_ZOOM } from "./config";
import { CopyToClipboard } from "react-copy-to-clipboard";
import { EditNode } from "./edit-node";
import { useFormik } from "formik";
import { Button } from "react-bootstrap";
import { TfiAngleLeft, TfiAngleRight } from "react-icons/tfi";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";



// TODO- get rid of this index when we introduce the new node modal
let NEW_NODE_INDEX = 1;

const NODE_TYPES = {
    "flypipe-node-existing": ExistingNode,
    "flypipe-node-new": NewNode,
};

const Graph = ({ nodeDefs: nodeDefsList, tagsSuggestions }) => {
    const [editNode, setEditNode] = useState(false);

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
    const formik = useFormik({
        initialValues: {
            name: "",
            nodeType: "",
        },
        validate,
        onSubmit: (values) => {
            alert(JSON.stringify(values, null, 2));
        },
    });

    const graph = useReactFlow();
    const nodeDefs = nodeDefsList.reduce(
        (accumulator, nodeDef) => ({
            ...accumulator,
            [nodeDef.nodeKey]: nodeDef,
        }),
        {}
    );

    const graphDiv = useRef(null);

    const onClickNewNode = useCallback(() => {
        const newNodeId = `new-node-${NEW_NODE_INDEX}`;
        const newNode = {
            id: newNodeId,
            type: "flypipe-node-new",
            data: {
                id: newNodeId,
                label: `Untitled-${NEW_NODE_INDEX}`,
                isNew: false,
                nodeType: "pandas",
                description: "",
                tags: [],
                output: [],
                predecessors: [],
                successors: [],
            },
            position: {
                // dummy position, this will be automatically updated later
                x: 0,
                y: 0,
            },
        };
        NEW_NODE_INDEX += 1;

        graph.addNodes(newNode);
        refreshNodePositions(graph);
        moveToNode(graph, newNodeId);

        formik.resetForm({
            values: newNode.data,
        });

        setEditNode(true);
    }, [graph]);

    const onCopyToClipboard = useCallback(() => {
        // TODO- add notification toast msg here
    }, []);

    const onNodeClick = useCallback((event, node) => {
        console.log("{...node.data, id: node.id, type: node.type}=>", {
            ...node.data,
            id: node.id,
            isNew: node.type === "flypipe-node-new" ? true : false,
        });
        formik.resetForm({
            values: {
                ...node.data,
                id: node.id,
                isNew: node.type === "flypipe-node-new" ? true : false,
            },
        });
        setEditNode(true);
    }, []);

    const [showSearchPanel, setShowSearchPanel] = useState(true);

    const toggleSearchPanel = () => {
        setShowSearchPanel(!showSearchPanel);
    }

    return (
        <div className="layoutflow" ref={graphDiv}>
            
            {editNode && (
                <EditNode formik={formik} tagsSuggestions={tagsSuggestions} />
            )}
            <ReactFlow
                defaultNodes={[]}
                defaultEdges={[]}
                nodeTypes={NODE_TYPES}
                minZoom={MIN_ZOOM}
                maxZoom={MAX_ZOOM}
                onNodeClick={onNodeClick}
                fitView
            >
                <Panel  className={`m-0 ${showSearchPanel ? 'search-show' : 'search-hide'}`}>
                    <Button 
                    variant="outline-secondary flypipe" 
                    className="search-toggle-button position-absolute top-50 p-2 pt-3 pb-3" 
                    size="sm"
                    onClick={toggleSearchPanel}>
                            {showSearchPanel && <TfiAngleLeft/>}
                            {!showSearchPanel && <TfiAngleRight/>}
                            
                    </Button>
                    <div className="row">
                        <div className="col search-result">
                            <Search nodes={nodeDefsList} />
                        </div>
                        <div className="col">
                            <OverlayTrigger
                                key="right"
                                placement="right"
                                overlay={
                                    <Tooltip id="tooltip-right">
                                        Add new node to the graph
                                    </Tooltip>
                                }
                            >
                                <Button variant="outline-secondary flypipe"  className="mt-2" onClick={onClickNewNode}>New Node</Button>
                            </OverlayTrigger>
                        </div>
                    </div>
                </Panel>
                <Panel position="top-right">
                    <OverlayTrigger
                                key="right"
                                placement="left"
                                overlay={
                                    <Tooltip id="tooltip-right">
                                        Copy edited nodes source code to the clipboard
                                    </Tooltip>
                                }
                            >
                        <Button
                        variant="outline-secondary flypipe"
                            data-toggle="tooltip"
                            size="md"
                        >
                            <CopyToClipboard
                                text="<dummy>"
                                onCopy={onCopyToClipboard}
                            >
                                <BsDownload />
                            </CopyToClipboard>
                        </Button>
                    </OverlayTrigger>
                </Panel>
                <Controls />
                <Panel position="bottom-center">
                    <a href="//flypipe.github.io/flypipe/" target="_blank" className="text-secondary text-decoration-none fs-5">Flypipe</a>
                </Panel>
                <MiniMap zoomable pannable />
                <Background color="#aaa" gap={16} />
            </ReactFlow>
        </div>
    );
};

export default Graph;
