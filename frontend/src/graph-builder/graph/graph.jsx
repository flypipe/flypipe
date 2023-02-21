import React, {useState, useCallback, useRef, useContext, useEffect} from 'react';
import ReactFlow, {useNodes, useReactFlow, applyNodeChanges} from 'reactflow';
import {ExistingNode, NewNode} from './node';
import { refreshNodePositions, moveToNode } from '../util';
import 'reactflow/dist/style.css';
import {MIN_ZOOM, MAX_ZOOM, NODE_WIDTH, NODE_HEIGHT} from './config';
import { EditNode } from './edit-node'; 
import { useFormik } from 'formik';

// TODO- get rid of this index when we introduce the new node modal
let NEW_NODE_INDEX = 1;


const NODE_TYPES = {
    "flypipe-node-existing": ExistingNode,
    "flypipe-node-new": NewNode
};

const Graph = ({nodeDefs: nodeDefsList, tagsSuggestions}) => {

    const [editNode, setEditNode] = useState(false);

    const validate = values => {
        const errors = {}

        if (!values.label){
            errors.label = 'Name is required'
        }

        if (!values.nodeType){
            errors.nodeType = 'Type is required'
        }

        return errors
    }
    const formik = useFormik({
        initialValues: {
          name: '',
          nodeType: '',
        },
        validate,
        onSubmit: values => {
          alert(JSON.stringify(values, null, 2));
        },
      });

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
                "id": newNodeId,
                "label": `Untitled-${NEW_NODE_INDEX}`,
                "nodeType": "",
                "sourceCode": "",
                "description": "",
                "tags": [],
                "output": [],
                "predecessors": [],
                "successors": []

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

        formik.resetForm({
            values: newNode.data
          });

          setEditNode(true);

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
            const id = {id: node.id}

            formik.resetForm({
                values: {...node.data, id: node.id}
            });
            setEditNode(true);
        },
        []
    );

    return (
        <div className="layoutflow" ref={graphDiv}>
            <div className="m-4">
                <button className="btn btn-secondary" onClick={onClickNewNode}>New Node</button>
            </div>
            { editNode && <EditNode formik={formik}  tagsSuggestions={tagsSuggestions}/>}
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
