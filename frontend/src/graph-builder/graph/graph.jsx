import React, {useState, useCallback, useRef, useContext, useEffect} from 'react';
import ReactFlow, {useNodes, useReactFlow, applyNodeChanges} from 'reactflow';
import {ExistingNode, NewNode} from './node';
import { refreshNodePositions, moveToNode } from '../util';
import 'reactflow/dist/style.css';
import {MIN_ZOOM, MAX_ZOOM, NODE_WIDTH, NODE_HEIGHT} from './config';
import { EditNode } from './edit-node'; 

// TODO- get rid of this index when we introduce the new node modal
let NEW_NODE_INDEX = 1;


const NODE_TYPES = {
    "flypipe-node-existing": ExistingNode,
    "flypipe-node-new": NewNode
};

const Graph = ({nodeDefs: nodeDefsList}) => {
    const [nodeInView, setNodeInView] = useState(null);
    const graph = useReactFlow();
    const nodeDefs = nodeDefsList.reduce((accumulator, nodeDef) => ({...accumulator, [nodeDef.nodeKey]: nodeDef}),{});

    const graphDiv = useRef(null);
    const onDragOver = useCallback((event) => {
        event.preventDefault();
        event.dataTransfer.dropEffect = 'move';
      }, []);


    const onClickNewNode = useCallback(() => {
        const newNodeId = `new-node-${NEW_NODE_INDEX}`;
        const newNode = {
            "id": newNodeId,
            "type": "flypipe-node-new",
            "data": {
                "label": `Untitled-${NEW_NODE_INDEX}`
            },
            "position": { // dummy position, this will be automatically updated later
                "x": 0,
                "y": 0,
            },
            "width": NODE_WIDTH,
            "height": NODE_HEIGHT,
        };
        NEW_NODE_INDEX += 1;

        graph.addNodes(newNode);
        refreshNodePositions(graph);
        moveToNode(graph, newNodeId);

        setNodeInView(graph.getNode(newNodeId));

    }, [graph]);
    
    const onDrop = useCallback(
        (event) => {
            event.preventDefault();
            const nodeKey = event.dataTransfer.getData('application/reactflow');
            const nodeDef = nodeDefs[nodeKey];
            let [newNodes, newEdges] = getNodeGraph(nodeDefs, nodeDef.nodeKey);
            addNodesAndEdges(newNodes, newEdges);
        },
        [nodeDefs]
    );

    const onNodeClick = useCallback(
        (event, node) => {
            setNodeInView(node);
        },
        []
    );
    // graph.fitView({duration: 250});

    const onNodeChanged =  useCallback(
        (name, value) => {
            console.log("node changed:", name, value);

            
            setNodeInView((prevNode) => {
                const node = {...prevNode};
                node.data[name] = value;
                return node;
            })
        },
        []
    );

    return (
        <div className="layoutflow" ref={graphDiv}>
            <div className="m-4">
                <button className="btn btn-secondary" onClick={onClickNewNode}>New Node</button>
            </div>
            { nodeInView && <EditNode node={nodeInView} onNodeChanged={onNodeChanged} />}
            <ReactFlow
                defaultNodes={[]}
                defaultEdges={[]}
                nodeTypes={NODE_TYPES}
                minZoom={MIN_ZOOM}
                maxZoom={MAX_ZOOM}
                onNodeDrag={(e) => {console.log(e)}}
                onNodeClick={onNodeClick}
                fitView
            />
        </div>
      );
}


export default Graph;
