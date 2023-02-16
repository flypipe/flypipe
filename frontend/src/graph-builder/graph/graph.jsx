import React, {useState, useCallback, useRef, useContext, useEffect} from 'react';
import ReactFlow, {useNodes, useReactFlow, applyNodeChanges} from 'reactflow';
import Node from './node';
import { useStore } from '../store';
import { shallow } from 'zustand/shallow';
import 'reactflow/dist/style.css';


// TODO- get rid of this index when we introduce the new node modal
let NEW_NODE_INDEX = 1
const MIN_ZOOM = 0.6;
const MAX_ZOOM = 2;


const NODE_TYPES = {
    "flypipe-node": Node
};

const Graph = ({nodeDefs: nodeDefsList}) => {
    const graph = useReactFlow();
    const nodeDefs = nodeDefsList.reduce((accumulator, nodeDef) => ({...accumulator, [nodeDef.nodeKey]: nodeDef}),{});
    const {nodes, edges, addNode, editNode, addNodesAndEdges} = useStore(({nodes, edges, addNode, editNode, addNodesAndEdges}) => ({
        nodes, 
        edges, 
        addNode, 
        editNode, 
        addNodesAndEdges
    }), shallow);

    const graphDiv = useRef(null);
    const onDragOver = useCallback((event) => {
        event.preventDefault();
        event.dataTransfer.dropEffect = 'move';
      }, []);


    const onClickNewNode = useCallback(() => {
        const newNode = {
            "id": `new-node-${NEW_NODE_INDEX}`,
            "type": "flypipe-node",
            "data": {
                "label": `Untitled-${NEW_NODE_INDEX}`
            },
            "position": { // dummy position, this will be automatically updated later
                "x": 0,
                "y": 0,
            }
        };
        NEW_NODE_INDEX += 1;
        addNode(newNode, []);
    }, []);
    
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
    useEffect(() => {
        graph.fitView({duration: 500});
    }, [nodes, edges, graph]);

    return (
        <div className="layoutflow" ref={graphDiv}>
            <div className="m-4">
                <button className="btn btn-secondary" onClick={onClickNewNode}>New Node</button>
            </div>
            <ReactFlow
                nodes={nodes}
                nodeTypes={NODE_TYPES}
                minZoom={MIN_ZOOM}
                maxZoom={MAX_ZOOM}
                edges={edges}
                onNodesChange={(changes) => {
                    // TODO do we always have exactly one change in the event??? 
                    if (changes[0].type === 'position') {
                        const {id, position} = changes[0];
                        editNode(id, {position});
                    }
                }}
                onDrop={onDrop}
                onDragOver={onDragOver}
                // connectionLineType={ConnectionLineType.Straight}
                fitView
            />
        </div>
      );
}


export default Graph;
