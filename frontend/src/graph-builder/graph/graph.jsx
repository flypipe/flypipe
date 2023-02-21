import React, {useCallback, useRef} from 'react';
import ReactFlow, {useReactFlow, Controls, Background, Panel, MiniMap} from 'reactflow';
import { BsDownload } from 'react-icons/bs';
import {ExistingNode, NewNode} from './node';
import { refreshNodePositions, moveToNode } from '../util';
import 'reactflow/dist/style.css';
import {MIN_ZOOM, MAX_ZOOM} from './config';
import {CopyToClipboard} from 'react-copy-to-clipboard';


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
    const onCopyToClipboard = useCallback(() => {
        // TODO- add notification toast msg here
    }, []);

    return (
        <div className="layoutflow" ref={graphDiv}>
            <ReactFlow
                defaultNodes={[]}
                defaultEdges={[]}
                nodeTypes={NODE_TYPES}
                minZoom={MIN_ZOOM}
                maxZoom={MAX_ZOOM}
                fitView
            >
                <Panel position="top-left">
                    <button className="btn btn-secondary m-4" onClick={onClickNewNode}>New Node</button>
                </Panel>
                <Panel position="top-right">
                    <button 
                        className="btn btn-secondary m-4"
                        data-toggle="tooltip" 
                        title={"Export new nodes to the clipboard"}
                    >
                        <CopyToClipboard text="<dummy>" onCopy={onCopyToClipboard}>
                            <BsDownload/>
                        </CopyToClipboard>
                    </button>
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
