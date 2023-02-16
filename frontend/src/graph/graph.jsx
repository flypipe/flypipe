import React, {useState, useCallback, useRef, useContext} from 'react';
import ReactFlow, {useNodes} from 'reactflow';
import Node from './node';
import { useStore } from './store';
import { shallow } from 'zustand/shallow';
import { EditNode } from './edit-node';
import 'reactflow/dist/style.css';
import Button from 'react-bootstrap/Button';



// TODO- get rid of this index when we introduce the new node modal
let NEW_NODE_INDEX = 1


const NODE_TYPES = {
    "flypipe-node": Node
};

// Retrieve the graph node representation of a node
const convertNodeDefToGraphNode = ({nodeKey, name}) => ({
    "id": nodeKey,
    "type": "flypipe-node",
    "data": {
        "label": name
    },
    "position": { // dummy position, this will be automatically updated later
        "x": 0,
        "y": 0,
    }
});


// Given a node, get the graph formed by grabbing all of it's predecessor nodes.
const getNodeGraph = (nodeDefs, nodeKey) => {
    const nodeDef = nodeDefs[nodeKey];
    const frontier = [...nodeDef.predecessors];
    const selectedNodeDefs = [nodeDef];
    const edges = [];
    const addedKeys = [nodeDef.nodeKey];
    while (frontier.length > 0) {
        const current = nodeDefs[frontier.pop()];
        if (!addedKeys.includes(current.nodeKey)) {
            addedKeys.push(current.nodeKey);
            selectedNodeDefs.push(current);
            for (const successor of current.successors) {
                frontier.push(successor);
                edges.push({
                    "id": `${successor}-${current.nodeKey}`,
                    "source": successor,
                    "target": current.nodeKey,
                });
            }
        }
    }
    return [selectedNodeDefs.map((nodeDef) => convertNodeDefToGraphNode(nodeDef)), edges];
};


const Graph = ({nodeDefs: nodeDefsList}) => {
    const nodeDefs = nodeDefsList.reduce((accumulator, nodeDef) => ({...accumulator, [nodeDef.nodeKey]: nodeDef}),{});
    const {nodes, edges, nodeKeys, edgeKeys, addGraphData} = useStore((state) => ({
        nodes: state.nodes,
        edges: state.edges,
        nodeKeys: state.nodeKeys,
        edgeKeys: state.edgeKeys,
        addGraphData: state.addGraphData,
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
        addGraphData([newNode], []);
        
        

        
        
    }, []);
    
    const onDrop = useCallback(
        (event) => {
            event.preventDefault();
            const nodeKey = event.dataTransfer.getData('application/reactflow');
            const nodeDef = nodeDefs[nodeKey];
            let [nodesToAdd, edgesToAdd] = getNodeGraph(nodeDefs, nodeDef.nodeKey);
            // We're only going to add nodes/edges that don't yet exist in the graph
            nodesToAdd = nodesToAdd.filter((node) => !nodeKeys.has(node.id));
            edgesToAdd = edgesToAdd.filter((edge) => !edgeKeys.has(edge.id));
            if (nodesToAdd || edgesToAdd) {
                addGraphData(nodesToAdd, edgesToAdd);
            }
        },
        [nodeKeys, edgeKeys, nodeDefs]
    );

    return (
        <div className="layoutflow" ref={graphDiv}>
            <div className="m-4">
                {/* <button className="btn btn-secondary" onClick={onClickNewNode}>New Node</button> */}
                <Button variant="secondary" onClick={onClickNewNode}>New Node</Button>
            </div>
            <EditNode/>
            <ReactFlow
                nodes={nodes}
                nodeTypes={NODE_TYPES}
                edges={edges}
                onDrop={onDrop}
                onDragOver={onDragOver}
                // connectionLineType={ConnectionLineType.Straight}
                fitView
            />
        </div>
      );
}


export default Graph;