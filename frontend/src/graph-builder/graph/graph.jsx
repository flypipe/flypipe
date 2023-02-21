import React, {useState, useCallback, useRef} from 'react';
import ReactFlow, {useReactFlow, Controls, Background, Panel, MiniMap} from 'reactflow';
import {ExistingNode, NewNode} from './node';
import { refreshNodePositions, moveToNode } from '../util';
import 'reactflow/dist/style.css';
import {MIN_ZOOM, MAX_ZOOM} from './config';


// TODO- get rid of this index when we introduce the new node modal
let NEW_NODE_INDEX = 1;


const NODE_TYPES = {
    "flypipe-node-existing": ExistingNode,
    "flypipe-node-new": NewNode
};

const Graph = ({nodeDefs: nodeDefsList}) => {
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
                "nodeType": "pandas",
                "label": `Untitled-${NEW_NODE_INDEX}`
            },
            "position": { // dummy position, this will be automatically updated later
                "x": 0,
                "y": 0,
            },
        };
        NEW_NODE_INDEX += 1;

        graph.addNodes(newNode);
        refreshNodePositions(graph);
        moveToNode(graph, newNodeId);
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

    return (
        <div className="layoutflow" ref={graphDiv}>
            <ReactFlow
                defaultNodes={[]}
                defaultEdges={[]}
                nodeTypes={NODE_TYPES}
                minZoom={MIN_ZOOM}
                maxZoom={MAX_ZOOM}
                onNodeDrag={(e) => {console.log(e)}}
                fitView
            >
                <Panel position="top-left">
                    <div className="d-flex flex-column">
                        <div className="m-4">
                            <button className="btn btn-secondary mx-2" onClick={onClickNewNode}>New Node</button>
                        </div>
                    </div>
                </Panel>
                <Controls />
                <Panel position="bottom-center"><a href="//flypipe.github.io/flypipe/">Flypipe</a></Panel>
                <MiniMap zoomable pannable />
                <Background color="#aaa" gap={16} />
            </ReactFlow>
        </div>
      );
}


export default Graph;
