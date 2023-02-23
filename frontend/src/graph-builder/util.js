import dagre from "dagre";
import { ANIMATION_SPEED, DEFAULT_ZOOM } from "./graph/config";

// This isn't the actual node width which appears to be dynamically calculated. It's a width that we must give for
// the dagre layout calculation.
export const NODE_WIDTH = 250;
export const NODE_HEIGHT = 75;

// Given the nodes & edges from the graph, construct a dagre graph to automatically assign
// the positions of the nodes.
const assignNodePositions = (nodes, edges) => {
    const dagreGraph = new dagre.graphlib.Graph();
    dagreGraph.setGraph({ rankdir: "LR" });
    dagreGraph.setDefaultEdgeLabel(() => ({}));
    nodes.forEach(({ id }) => {
        dagreGraph.setNode(id, { width: NODE_WIDTH, height: NODE_HEIGHT });
    });

    edges.forEach(({ source, target }) => {
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

const refreshNodePositions = (graph) => {
    const nodes = graph.getNodes();
    assignNodePositions(nodes, graph.getEdges());
    graph.setNodes(nodes);
};

// Retrieve the graph node representation of a node
const convertNodeDefToGraphNode = (
    nodeDef,
    isNew = true
) => {
    return {
        id: nodeDef.nodeKey,
        type: isNew ? "flypipe-node-new" : "flypipe-node-existing",
        data: {
            label: nodeDef.name,
            isNew,
            ...nodeDef,
        },
        position: {
            // dummy position, this will be automatically updated later
            x: 0,
            y: 0,
        },
    }
};

// Given an input node, get the list of nodes and edges of all of the input node's predecessors.
const getPredecessorNodesAndEdgesFromNode = (nodeDefs, nodeKey) => {
    const nodeDef = nodeDefs.find((nodeDef) => nodeDef.nodeKey === nodeKey);
    const frontier = [...nodeDef.predecessors];
    const selectedNodeDefs = [nodeDef];
    const edges = [];
    const edgeKeys = new Set();
    const addedKeys = [nodeDef.nodeKey];
    while (frontier.length > 0) {
        const currentKey = frontier.pop();
        const current = nodeDefs.find(
            (nodeDef) => nodeDef.nodeKey === currentKey
        );
        if (!addedKeys.includes(current.nodeKey)) {
            addedKeys.push(current.nodeKey);
            selectedNodeDefs.push(current);
        }
        for (const successor of current.successors) {
            if (addedKeys.includes(successor)) {
                const edgeKey = `${current.nodeKey}-${successor}`;
                if (!edgeKeys.has(edgeKey)) {
                    const edge = {
                        id: edgeKey,
                        source: current.nodeKey,
                        target: successor,
                    };
                    edges.push(edge);
                    edgeKeys.add(edgeKey);
                }
            }
        }
        for (const predecessor of current.predecessors) {
            frontier.push(predecessor);
        }
    }
    return [
        selectedNodeDefs.map((nodeDef) =>
            convertNodeDefToGraphNode(nodeDef, false)
        ),
        edges,
    ];
};

const moveToNode = (graph, nodeId) => {
    const newNodePosition = graph
        .getNodes()
        .find(({ id }) => id === nodeId).position;
    graph.setCenter(newNodePosition.x, newNodePosition.y, {
        duration: ANIMATION_SPEED,
        zoom: DEFAULT_ZOOM,
    });
};

const generateCodeTemplate = (nodeData) => {
    const {name, nodeType, description, tags, predecessors, predecessors2} = nodeData;
    const argList = predecessors.join(', ');
    const tagList = tags.map(tag => `'${tag}'`).join(', ');
    const dependencyList = predecessors.join(', ');
    return `@node(
    type="${nodeType}",
    description="${description}",
    tags=[${tagList}],
    dependencies=[${dependencyList}]
)
def ${name}(${argList}):
    # <implement logic here>
`;
};

const getNewNodeDef = ({id, name, nodeType, description, tags, output, predecessors, successors}) => ({
    id,
    label: name || 'Untitled',
    isNew: true,
    name: name || 'Untitled',
    nodeType: nodeType || "pandas",
    description: description || "",
    tags: tags || [],
    output: output || [],
    predecessors: predecessors || [],
    successors: successors || [],
});

const addEdge = (graph, edge) => {
    const nodes = graph.getNodes();
    const sourceNode = nodes.splice(nodes.findIndex(node => node.id === edge.source), 1)[0];
    const targetNode = nodes.splice(nodes.findIndex(node => node.id === edge.target), 1)[0];
    if (!edge.id) {
        edge.id = `${edge.source}-${edge.target}`;
    }
    if (sourceNode.data.successors.includes(targetNode.id)) {
        throw new Error(`${targetNode.data.label} already has a dependency on ${sourceNode.data.label}`);
    }
    graph.addEdges(edge);

    // Update the predecessor/successor fields on the nodes the new edge is going between
    sourceNode.data.successors = [...sourceNode.data.successors, targetNode.id];
    targetNode.data.predecessors = [...targetNode.data.predecessors, sourceNode.id];
    graph.setNodes([sourceNode, targetNode, ...nodes]);
};

export {
    getPredecessorNodesAndEdgesFromNode,
    refreshNodePositions,
    moveToNode,
    generateCodeTemplate,
    getNewNodeDef,
    addEdge,
};
