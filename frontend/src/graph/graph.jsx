import React, {useState, useCallback, useRef} from 'react';
import ReactFlow, {useNodes} from 'reactflow';
import dagre from 'dagre';
import 'reactflow/dist/style.css';


const NODE_WIDTH = 172;
const NODE_HEIGHT = 36;

// Retrieve the graph node representation of a node
const convertNodeDefToGraphNode = ({nodeKey, name}) => ({
    "id": nodeKey,
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


// Given the nodes & edges from the graph, construct a dagre graph to automatically assign 
// the positions of the nodes. 
const assignNodePositions = (nodes, edges) => {
    const dagreGraph = new dagre.graphlib.Graph();
    dagreGraph.setGraph({ rankdir: 'LR' });
    dagreGraph.setDefaultEdgeLabel(() => ({}));
    nodes.forEach(({id}) => {
        dagreGraph.setNode(id, { width: NODE_WIDTH, height: NODE_HEIGHT });
    });

    edges.forEach(({source, target}) => {
        dagreGraph.setEdge(source, target);
    });
    dagre.layout(dagreGraph);

    nodes.forEach((node) => {
        const nodeWithPosition = dagreGraph.node(node.id);

        // We are shifting the dagre node position (anchor=center center) to the top left
        // so it matches the React Flow node anchor point (top left).
        node.position = {
            x: nodeWithPosition.x - NODE_WIDTH / 2,
            y: nodeWithPosition.y - NODE_HEIGHT / 2,
        };
    });
};



const Graph = ({nodeDefs: nodeDefsList}) => {
    const nodeDefs = nodeDefsList.reduce((accumulator, nodeDef) => ({...accumulator, [nodeDef.nodeKey]: nodeDef}),{});
    const [graphData, setGraphData] = useState({
        nodes: [],
        edges: [],
        nodeKeys: new Set(),
        edgeKeys: new Set(),
    });

    const graphDiv = useRef(null);
    const onDragOver = useCallback((event) => {
        event.preventDefault();
        event.dataTransfer.dropEffect = 'move';
      }, []);
    
    const onDrop = useCallback(
        (event) => {
            event.preventDefault();
            const nodeKey = event.dataTransfer.getData('application/reactflow');
            const nodeDef = nodeDefs[nodeKey];
            let [nodesToAdd, edgesToAdd] = getNodeGraph(nodeDefs, nodeDef.nodeKey);
            // We're only going to add nodes/edges that don't yet exist in the graph
            nodesToAdd = nodesToAdd.filter((node) => !graphData.nodeKeys.has(node.id));
            edgesToAdd = edgesToAdd.filter((edge) => !graphData.edgeKeys.has(edge.id));
            if (nodesToAdd || edgesToAdd) {
                setGraphData(prevGraphData => {
                    const newNodes = [...prevGraphData.nodes, ...nodesToAdd];
                    const newEdges = [...prevGraphData.edges, ...edgesToAdd];
                    const newGraphData = {
                        ...prevGraphData,
                        nodes: newNodes,
                        edges: newEdges,
                        nodeKeys: new Set(newNodes.map(node => node.id)),
                        edgeKeys: new Set(newEdges.map(edge => edge.id))
                    };
                    // Recalculate the positions of the nodes
                    assignNodePositions(newGraphData.nodes, newGraphData.edges);

                    return newGraphData;
                });
            }
        },
        [graphData, nodeDefs]
    );

    return (
        <div className="layoutflow" ref={graphDiv}>
          <ReactFlow
            nodes={graphData.nodes}
            edges={graphData.edges}
            onDrop={onDrop}
            onDragOver={onDragOver}
            // connectionLineType={ConnectionLineType.Straight}
            fitView
          />
        </div>
      );
}


export default Graph;