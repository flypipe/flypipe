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
import { refreshNodePositions, moveToNode, getNewNodeDef } from "../util";
import "reactflow/dist/style.css";
import { MIN_ZOOM, MAX_ZOOM } from "./config";
import { EditNode } from "./edit-node";
import { EditEdge } from "./edit-edge";
import { Button } from "react-bootstrap";
import { TfiAngleLeft, TfiAngleRight } from "react-icons/tfi";
import ExportGraph from "./export-graph";
import Tooltip from "../../tooltip";
import { NotificationContext } from "../../context";
import uuid from "react-uuid";
import { GraphContext } from "./graph-context";

// TODO- get rid of this index when we introduce the new node modal
let NEW_NODE_INDEX = 1;

const NODE_TYPES = {
    "flypipe-node-existing": ExistingNode,
    "flypipe-node-new": NewNode,
};

const Graph = ({ nodeDefs, tagSuggestions }) => {
    const { currentGraphObject, setCurrentGraphObject } =
        useContext(GraphContext);
    const { setNewMessage } = useContext(NotificationContext);
    const [showOffcanvas, setShowOffcanvas] = useState(false);

    const graph = useReactFlow();

    const graphDiv = useRef(null);

    const onClickNewNode = useCallback(() => {
        const newNodeId = `new-node-${NEW_NODE_INDEX}`;
        const newNode = {
            id: newNodeId,
            type: "flypipe-node-new",
            data: getNewNodeDef({
                nodeKey: newNodeId,
                label: `untitled${NEW_NODE_INDEX}`,
                name: `untitled${NEW_NODE_INDEX}`,
            }),
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

        setCurrentGraphObject({
            object: newNode,
            type: "node",
        });
        setNewMessage({
            msgId: uuid(),
            message: `New node ${newNode.data.label} added to the graph`,
        });
    }, [graph, setNewMessage, setCurrentGraphObject]);

    const onNodeClick = useCallback(
        (event, node) => {
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
            setCurrentGraphObject({
                object: edge,
                type: "edge",
            });
        },
        [setCurrentGraphObject]
    );

    useEffect(() => {
        setShowOffcanvas(true);
    }, [currentGraphObject.object, setShowOffcanvas]);

    const handleCloseEditEdge = useCallback(() => {
        setShowOffcanvas(false);
    }, [setShowOffcanvas]);

    return (
        <div className="layoutflow d-flex w-100 h-100" ref={graphDiv}>
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
            <ReactFlow
                defaultNodes={[]}
                defaultEdges={[]}
                nodeTypes={NODE_TYPES}
                minZoom={MIN_ZOOM}
                maxZoom={MAX_ZOOM}
                onNodeClick={onNodeClick}
                onEdgeClick={onEdgeClick}
                fitView
            >
                <Panel position="top-left">
                    <Tooltip text="Add a new node to the graph">
                        <Button
                            variant="outline-secondary flypipe"
                            className="mt-2"
                            onClick={onClickNewNode}
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
                <Panel position="top-right">
                    <ExportGraph />
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
    );
};

export default Graph;
