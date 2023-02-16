import dagre from 'dagre';

const NODE_WIDTH = 172;
const NODE_HEIGHT = 36;

// Given the nodes & edges from the graph, construct a dagre graph to automatically assign 
// the positions of the nodes. 
export const assignNodePositions = (nodes, edges) => {
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

// Given an input node, get the list of nodes and edges of all of the input node's predecessors. 
export const getPredecessorNodesAndEdgesFromNode = (nodeDefs, nodeKey) => {
    const nodeDef = nodeDefs.find((nodeDef) => nodeDef.nodeKey === nodeKey);
    const frontier = [...nodeDef.predecessors];
    const selectedNodeDefs = [nodeDef];
    const edges = [];
    const addedKeys = [nodeDef.nodeKey];
    while (frontier.length > 0) {
        const currentKey = frontier.pop();
        const current = nodeDefs.find((nodeDef) => nodeDef.nodeKey === currentKey);
        if (!addedKeys.includes(current.nodeKey)) {
            addedKeys.push(current.nodeKey);
            selectedNodeDefs.push(current);
            for (const successor of current.successors) {
                frontier.push(successor);
                edges.push({
                    "source": successor,
                    "target": current.nodeKey,
                });
            }
        }
    }
    return [selectedNodeDefs.map((nodeDef) => convertNodeDefToGraphNode(nodeDef)), edges];
};