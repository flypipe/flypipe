import React, { useCallback, useRef, useState } from "react";
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


// TODO- get rid of this index when we introduce the new node modal
let NEW_NODE_INDEX = 1;

const NODE_TYPES = {
    "flypipe-node-existing": ExistingNode,
    "flypipe-node-new": NewNode,
};

const Graph = ({ nodeDefs: nodeDefsList, tagSuggestions }) => {
    const [editNode, setEditNode] = useState(null);
    const [showEditNode, setShowEditNode] = useState(false);

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
            data: getNewNodeDef({
                id: newNodeId,
                label: `Untitled-${NEW_NODE_INDEX}`,
                isNew: false,
                nodeType: "pandas",
                description: "",
                tags: [],
                output: [],
                predecessors: [],
                predecessor_columns: {},
                successors: [],
                name: `Untitled${NEW_NODE_INDEX}`,
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

        setEditNode(newNode);
        setShowEditNode(true);
    }, [graph]);

    const onNodeClick = useCallback((event, node) => {
        setEditNode(node);
        setShowEditNode(true);
    }, []);

    // Edge edition
    const [showEditEdge, setShowEditEdge] = useState(false);
    const [editEdge, setEditEdge] = useState(false);
    
    const onEdgeClick = useCallback((event, edge) => {
        setShowEditEdge(true);
        setEditEdge(edge);        

    }, [graph]);

    // Show/Hide Search Panel
    const [showSearchPanel, setShowSearchPanel] = useState(true);

    const toggleSearchPanel = () => {
        setShowSearchPanel(!showSearchPanel);
    }

    const handleCloseEditNode = useCallback(() => {
        setShowEditNode(false);
    }, [setShowEditNode]);
    const handleSaveEditNode = useCallback((editedNode) => {
        setShowEditNode(false);
        const nodes = graph.getNodes();
        const newNodes = nodes.reduce((accumulator, node) => {
            if (node.id !== editedNode.id) {
                return [...accumulator, node];
            } else {
                return [...accumulator, {...node, data: {...editedNode}}];
            }
        }, []);
        graph.setNodes(newNodes);
    }, [setShowEditNode]);

    return (
        <div className="layoutflow" ref={graphDiv}>
            {showEditEdge && <EditEdge edge={editEdge} setShowEditEdge={setShowEditEdge}/>}
            {showEditNode && (
                <EditNode node={editNode} tagSuggestions={tagSuggestions} onClose={handleCloseEditNode} onSave={handleSaveEditNode}/>
            )}
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
                <Panel className={`m-0 ${showSearchPanel ? 'search-show' : 'search-hide'}`}>
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
                            <Tooltip text="Add a new node to the graph">
                                <Button variant="outline-secondary flypipe"  className="mt-2" onClick={onClickNewNode}>New Node</Button>
                            </Tooltip>
                        </div>
                    </div>
                </Panel>
                <Panel position="top-right">
                    <ExportGraph/>
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
