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