import React, {
    useCallback,
    useRef,
    useState,
    useContext,
    useEffect,
} from "react";
import Search from "../search/search";
import ReactFlow, {
    useReactFlow,
    Controls,
    Background,
    Panel,
    MiniMap,
} from "reactflow";
import { ExistingNode, NewNode } from "./node";
import {
    moveToNode,
    getNewNodeDef,
    addNodeAndPredecessors,
    NODE_WIDTH,
    NODE_HEIGHT,
} from "../util";
import "reactflow/dist/style.css";
import { MIN_ZOOM, MAX_ZOOM } from "./config";
import { EditNode } from "./edit-node";
import { EditEdge } from "./edit-edge";
import { Button } from "react-bootstrap";
import { TfiAngleLeft, TfiAngleRight } from "react-icons/tfi";
import ExportGraph from "./export-graph";
import Tooltip from "../tooltip";
import { NotificationContext } from "../notifications/context";
import uuid from "react-uuid";
import { GraphContext } from "./graph-context";
import { ClearGraph } from "./delete/delete";
import GroupNode from "./group";

// TODO- get rid of this index when we introduce the new node modal
let NEW_NODE_INDEX = 1;

const NODE_TYPES = {
    "flypipe-node-existing": ExistingNode,
    "flypipe-node-new": NewNode,
    "flypipe-group": GroupNode,
};

const Graph = ({ initialNodes, nodeDefs, groupDefs, tagSuggestions }) => {
    const { currentGraphObject, setCurrentGraphObject } =
        useContext(GraphContext);
    const { setNewMessage } = useContext(NotificationContext);
    const [showOffcanvas, setShowOffcanvas] = useState(false);

    const graph = useReactFlow();
    const graphDivRef = useRef(null);

    const handleInit = useCallback(() => {
        groupDefs.forEach((group) => {
            graph.addNodes({
                id: `${group.id}-minimised`,
                type: "flypipe-group",
                hidden: true,
                zIndex: -1001,
                data: {
                    label: group.name,
                    isMinimised: true,
                    nodes: new Set(),
                },
                position: {
                    // dummy position, this will be automatically updated later
                    x: 0,
                    y: 0,
                },
                style: {
                    width: 0,
                    height: 0,
                },
            });
        });
        if (initialNodes.length > 0) {
            initialNodes.forEach((nodeKey) =>
                addNodeAndPredecessors(graph, nodeDefs, nodeKey)
            );
            moveToNode(graph, initialNodes[initialNodes.length - 1]);
        }
    }, [groupDefs, initialNodes, graph]);

    const handleDragStart = useCallback((event) => {
        event.dataTransfer.effectAllowed = "copy";
    }, []);
    const handleDragOver = useCallback((event) => {
        event.preventDefault();
        event.dataTransfer.dropEffect = "copy";
    }, []);
    const handleDropNewNode = useCallback(
        (event) => {
            const { x: graphX, y: graphY } =
                graphDivRef.current.getBoundingClientRect();
            const cursorPosition = {
                x: event.clientX - graphX,
                y: event.clientY - graphY,
            };
            const graphPosition = graph.project(cursorPosition);
            graphPosition.x -= NODE_WIDTH / 2;
            graphPosition.y -= NODE_HEIGHT / 2;

            // Add a new node to the graph at the cursor's position
            const newNodeId = `new-node-${NEW_NODE_INDEX}`;
            const newNode = {
                id: newNodeId,
                type: "flypipe-node-new",
                parentNode: "",
                extent: "parent",
                data: getNewNodeDef({
                    nodeKey: newNodeId,
                    label: `untitled${NEW_NODE_INDEX}`,
                    name: `untitled${NEW_NODE_INDEX}`,
                }),
                position: graphPosition,
                style: {
                    width: NODE_WIDTH,
                    height: NODE_HEIGHT,
                },
            };
            NEW_NODE_INDEX += 1;
            graph.addNodes(newNode);

            setCurrentGraphObject({
                object: newNode,
                type: "node",
            });
            setNewMessage({
                msgId: uuid(),
                message: `New node ${newNode.data.label} added to the graph`,
            });
        },
        [graphDivRef, graph, setCurrentGraphObject, setNewMessage]
    );

    const onNodeClick = useCallback(
        (event, node) => {
            if (node.type === "flypipe-group") {
                // Ignore group nodes
                return;
            }
            setCurrentGraphObject({
                object: node,
                type: "node",
            });
        },
        [setCurrentGraphObject]
    );

    // Show/Hide Search Panel
    const [showSearchPanel, setShowSearchPanel] = useState(true);
    const toggleSearchPanel = useCallback(() => {
        setShowSearchPanel((prevShowSearchPanel) => !prevShowSearchPanel);
    }, [setShowSearchPanel]);

    const handleCloseEditNode = useCallback(() => {
        setShowOffcanvas(false);
    }, [setShowOffcanvas]);
    const handleSaveEditNode = useCallback(
        (editedNode) => {
            setShowOffcanvas(false);
            const nodes = graph.getNodes();
            const newNodes = nodes.reduce((accumulator, node) => {
                if (node.id !== editedNode.nodeKey) {
                    return [...accumulator, node];
                } else {
                    return [
                        ...accumulator,
                        { ...node, data: { ...editedNode } },
                    ];
                }
            }, []);
            graph.setNodes(newNodes);
        },
        [setShowOffcanvas]
    );

    const onEdgeClick = useCallback(
        (event, edge) => {
            // Edges that link directly to minimised group nodes are placeholders to substitute for the edges to the
            // nodes inside the group which are invisible. We should use the linked edge instead of the physical edge
            // in such cases.
            const currentEdge = edge.linkedEdge
                ? graph.getEdges().find(({ id }) => id === edge.linkedEdge)
                : edge;
            setCurrentGraphObject({
                object: currentEdge,
                type: "edge",
            });
        },
        [graph, setCurrentGraphObject]
    );

    useEffect(() => {
        setShowOffcanvas(true);
    }, [currentGraphObject, setShowOffcanvas]);

    const handleCloseEditEdge = useCallback(() => {
        setShowOffcanvas(false);
    }, [setShowOffcanvas]);

    return (
        <div className="d-flex w-100 h-100 align-items-stretch">
            {showOffcanvas && currentGraphObject.type === "edge" && (
                <EditEdge
                    edge={currentGraphObject.object}
                    onClose={handleCloseEditEdge}
                />
            )}
            {showOffcanvas && currentGraphObject.type === "node" && (
                <EditNode
                    node={currentGraphObject.object}
                    tagSuggestions={tagSuggestions}
                    onClose={handleCloseEditNode}
                    onSave={handleSaveEditNode}
                />
            )}
            {showSearchPanel && <Search nodes={nodeDefs} />}
            {/* Don't remove this div, there's this weird bug with the graph having height 0 in jupyter notebooks that this fixes */}
            <div className="w-100">
                <ReactFlow
                    defaultNodes={[]}
                    defaultEdges={[]}
                    onInit={handleInit}
                    nodeTypes={NODE_TYPES}
                    minZoom={MIN_ZOOM}
                    maxZoom={MAX_ZOOM}
                    onNodeClick={onNodeClick}
                    onEdgeClick={onEdgeClick}
                    onDrop={handleDropNewNode}
                    onDragOver={handleDragOver}
                    fitView
                    ref={graphDivRef}
                >
                    <Panel position="top-left" className="mt-2">
                        <Tooltip text="Click & drag to add a new node to the graph in a particular position">
                            <Button
                                variant="outline-secondary flypipe"
                                onDragStart={handleDragStart}
                                draggable
                            >
                                New Node
                            </Button>
                        </Tooltip>
                    </Panel>
                    <Panel position="bottom-left" className="m-0">
                        <div className="d-flex">
                            <Button
                                variant="outline-secondary flypipe"
                                className="search-toggle-button p-2 my-3 py-3 align-self-end"
                                size="sm"
                                onClick={toggleSearchPanel}
                            >
                                {showSearchPanel ? (
                                    <TfiAngleLeft />
                                ) : (
                                    <TfiAngleRight />
                                )}
                            </Button>
                            <Controls className="position-relative" />
                        </div>
                    </Panel>
                    <Panel position="top-right" className="mt-2">
                        <div className="d-flex">
                            <div className="me-2">
                                <ClearGraph />
                            </div>
                            <ExportGraph />
                        </div>
                    </Panel>

                    <Panel position="bottom-center">
                        <a
                            href="//flypipe.github.io/flypipe/"
                            target="_blank"
                            className="text-secondary text-decoration-none fs-5"
                        >
                            Flypipe
                        </a>
                    </Panel>
                    <MiniMap zoomable pannable />
                    <Background color="#aaa" gap={16} />
                </ReactFlow>
            </div>
        </div>
    );
};

export default Graph;
