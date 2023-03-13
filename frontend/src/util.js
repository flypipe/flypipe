import dagre from "dagre";
import { ANIMATION_SPEED, DEFAULT_ZOOM } from "./graph/config";
import { MarkerType } from "reactflow";

export const NODE_WIDTH = 250;
export const NODE_HEIGHT = 75;

// Given the nodes & edges from the graph, construct a dagre graph to automatically assign
// the positions of the nodes.
const assignNodePositions = (nodes, edges) => {
    const dagreGraph = new dagre.graphlib.Graph();
    console.info("Start dagre");
    dagreGraph.setGraph({ rankdir: "LR", nodesep: 100, ranksep: 150 });
    dagreGraph.setDefaultEdgeLabel(() => ({}));
    nodes.forEach(({ id, style }) => {
        const { width, height } = style;
        console.info(`Add node ${id} to dagre w,h=${width}, ${height}`);
        dagreGraph.setNode(id, { width: 250, height: 75 });
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
        console.log(`Position of node ${node.id}: ${node.position.x}, ${node.position.y}`);
    });
    console.info("End dagre");
};

const refreshNodePositions = (graph) => {
    // Exclude new nodes that have no edges, the idea being that after a new node is dragged into the graph the
    // position remains under the user's discretion until it is connected into other nodes.
    const calculatedNodes = [];
    const ignoredNodes = [];
    graph.getNodes().forEach((node) => {
        if (
            node.type !== 'group' &&
            node.data.isNew &&
            node.data.predecessors.length === 0 &&
            node.data.successors.length === 0
        ) {
            ignoredNodes.push(node);
        } else {
            calculatedNodes.push(node);
        }
    });

    const groups = calculatedNodes.filter(({type}) => type === "group");
    const nodes = calculatedNodes.filter(({type}) => type !== "group");
    groups.forEach(group => {
        const groupNodes = nodes.filter(({id}) => group.data.nodes.has(id));
        const groupEdges = graph.getEdges().filter(({source, target}) => group.data.nodes.has(source) || group.data.nodes.has(target));
        assignNodePositions(
            groupNodes, 
            groupEdges
        );
        group.position = {
            x: group.position.x - 50,
            y: group.position.y - 50,
        }
        group.style = {
            width: Math.max(...groupNodes.map(groupNode => groupNode.position.x + groupNode.style.width + 50)),
            height: Math.max(...groupNodes.map(groupNode => groupNode.position.y + groupNode.style.height + 50))
        };
    });
    const groupsAndUngroupedNodes = calculatedNodes.filter(node => node.type === "group" || !node.data.group);
    assignNodePositions(
        groupsAndUngroupedNodes,
        graph.getEdges().filter(
            ({source, target}) => (
                groupsAndUngroupedNodes.map(({id}) => id).includes(source) || 
                groupsAndUngroupedNodes.map(({id}) => id).includes(target)
            )
        )
    );
    graph.setNodes([...calculatedNodes, ...ignoredNodes]);
};

const refreshGroups = (graph) => {
    const nodes = graph.getNodes().filter(({type}) => type !== "group");
    const groups = [...graph.getNodes().filter(({type}) => type === "group")];    

    const edges = graph.getEdges();

    // Handle group expansion/minimisation
    groups.forEach(group => {
        if (group.data.isExpanded) {
            group.data.nodes.forEach(nodeId => {
                const node = nodes.find(n => n.id === nodeId);
                node.hidden = false;
            });
            edges.filter(({source, target}) => source === group.id || target === group.id).forEach(edge => {
                if (edge.source === group.id) {
                    edge.source = edge.tmp;
                } else if (edge.target === group.id) {
                    edge.target = edge.tmp;
                }
            });
        } else {
            // If a group is minimised, then all the nodes inside the graph should be hidden, any incoming edges to these 
            // nodes should be replaced by edges targeting the group node, and any outgoing edges from these nodes should be 
            // replaced by edges originating from the group node. 
            group.data.nodes.forEach(nodeId => {
                const node = nodes.find(n => n.id === nodeId);
                node.hidden = true;
                edges.filter(({source, target}) => source === nodeId || target === nodeId).forEach(edge => {
                    if (edge.source === nodeId) {
                        edge.tmp = nodeId;
                        edge.source = group.id;
                    } else if (edge.target === nodeId) {
                        edge.tmp = nodeId;
                        edge.target = group.id;
                    }
                });
            });
        }
    });

    graph.setNodes([...groups, ...nodes]);
};

// Retrieve the graph node representation of a node
const convertNodeDefToGraphNode = (nodeDef, isNew = true) => {
    return {
        id: nodeDef.nodeKey,
        type: isNew ? "flypipe-node-new" : "flypipe-node-existing",
        parentNode: nodeDef.group || "",
        data: {
            nodeKey: nodeDef.nodeKey,
            label: nodeDef.name,
            isNew,
            ...nodeDef,
        },
        position: {
            // dummy position, this will be automatically updated later
            x: 0,
            y: 0,
        },
        style: {
            width: NODE_WIDTH,
            height: NODE_HEIGHT,
        }
    };
};

// Given an input node, get the list of nodes and edges of all of the input node's predecessors.
const getPredecessorNodesAndEdges = (nodeDefs, nodeKey) => {
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
        for (const successorKey of current.successors) {
            if (addedKeys.includes(successorKey)) {
                const edgeKey = `${current.nodeKey}-${successorKey}`;
                if (!edgeKeys.has(edgeKey)) {
                    const successor = nodeDefs.find(
                        (nodeDef) => nodeDef.nodeKey === successorKey
                    );
                    const edge = {
                        id: edgeKey,
                        isNew: false,
                        source: current.nodeKey,
                        target: successorKey,
                        markerEnd: {
                            type: MarkerType.ArrowClosed,
                            width: 20,
                            height: 20,
                        },
                        ...(successor.isActive || {
                            style: {
                                strokeDasharray: "5,5",
                            },
                        }),
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

const addNodeAndPredecessors = (graph, nodeDefs, nodeKey) => {
    const [predecessorNodes, predecessorEdges] = getPredecessorNodesAndEdges(
        nodeDefs,
        nodeKey
    );
    // Add the node and any predecessor nodes/edges to the graph that aren't already there.
    const currentNodes = new Set(graph.getNodes().map(({ id }) => id));
    const currentEdges = new Set(graph.getEdges().map(({ id }) => id));
    const newNodes = predecessorNodes.filter(({ id }) => !currentNodes.has(id));
    const newEdges = predecessorEdges.filter(({ id }) => !currentEdges.has(id));
    const nodes = graph.getNodes().filter(({type}) => type !== "group");
    const groups = graph.getNodes().filter(({type}) => type === "group");

    newNodes.forEach(node => {
        const groupId = node.data.group;
        if (groupId) {
            if (!groups.find(({id}) => id === groupId)) {
                // If any of the new nodes are in groups that don't yet exist in the graph we'll need to create them
                groups.push({
                    id: groupId,
                    type: "group",
                    data: {
                        label: groupId,
                        isExpanded: false,
                        nodes: new Set([node.id]),
                    },
                    position: {
                        // dummy position, this will be automatically updated later
                        x: 0,
                        y: 0,
                    },
                })
            } else {
                groups.find(({id}) => id === groupId).data.nodes.add(node.id);
            }
        }
    });

    // debugger;

    graph.setNodes([...groups, ...nodes, ...newNodes]);
    graph.addEdges(newEdges);

    // refreshGroups(graph);
    refreshNodePositions(graph);
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

const generateCodeTemplate = (graph, nodeData) => {
    const {
        name,
        nodeType,
        description,
        tags,
        predecessors,
        predecessorColumns,
    } = nodeData;
    const importList = [
        "from flypipe import node",
        ...predecessors
            .map((nodeId) => graph.getNode(nodeId).data.importCmd)
            .filter((importCmd) => importCmd !== ""),
    ];
    const imports = importList.join("\n");

    const tagList = tags.map(({ text }) => `'${text}'`).join(", ");
    const dependencyList = predecessors
        .map((nodeId) => {
            const name = graph.getNode(nodeId).data.name;
            if (predecessorColumns[nodeId].length > 0) {
                const selectedColumns = predecessorColumns[nodeId]
                    .map((column) => `'${column}'`)
                    .join(", ");
                return `${name}.select(${selectedColumns})`;
            } else {
                return name;
            }
        })
        .join(", ");
    const nodeParameters = [
        `type='${nodeType}'`,
        ...(description ? [`description='${description}'`] : []),
        ...(tagList.length > 0 ? [`tags=[${tagList}]`] : []),
        ...(dependencyList.length > 0
            ? [`dependencies=[${dependencyList}]`]
            : []),
    ];

    const argumentList = predecessors
        .map((nodeId) => graph.getNode(nodeId).data.name)
        .join(", ");

    return `${imports}
    
@node(
    ${nodeParameters.join(",\n    ")}
)
def ${name}(${argumentList}):
    # <implement logic here>
`;
};

const getNewNodeDef = ({
    nodeKey,
    name,
    nodeType,
    description,
    tags,
    output,
    predecessors,
    predecessorColumns,
    successors,
    sourceCode,
    isActive,
}) => ({
    nodeKey,
    label: name || "Untitled",
    isNew: true,
    name: name || "Untitled",
    nodeType: nodeType || "pandas",
    description: description || "",
    tags: tags || [],
    output: output || [],
    predecessors: predecessors || [],
    predecessorColumns: predecessorColumns || [],
    successors: successors || [],
    sourceCode: sourceCode || "",
    isActive: isActive || true,
});

// This is a bit weird- as far as I can tell when dragging an edge between two nodes you can't suppress the automatic
// edge creation so as to add an edge with custom requirements. Therefore we add the edge and immediately consolidate
// it by tweaking it to have the settings we want.
const consolidateEdge = (graph, edge) => {
    const nodes = graph.getNodes();
    const sourceNode = nodes.splice(
        nodes.findIndex((node) => node.id === edge.source),
        1
    )[0];
    const targetNode = nodes.splice(
        nodes.findIndex((node) => node.id === edge.target),
        1
    )[0];

    // Update the predecessor/successor fields on the nodes the new edge is going between
    sourceNode.data.successors = [...sourceNode.data.successors, targetNode.id];
    targetNode.data.predecessors = [
        ...targetNode.data.predecessors,
        sourceNode.id,
    ];
    targetNode.data.predecessorColumns = {
        ...targetNode.data.predecessorColumns,
        [sourceNode.id]: [],
    };
    graph.setNodes([sourceNode, targetNode, ...nodes]);

    const otherEdges = graph.getEdges().filter(({ id }) => id !== edge.id);
    edge.id = `${edge.source}-${edge.target}`;
    edge.isNew = true;
    edge.markerEnd = {
        type: MarkerType.ArrowClosed,
        width: 20,
        height: 20,
    };
    if (!targetNode.data.isActive) {
        edge.style = {
            strokeDasharray: "5,5",
        };
    }

    graph.setEdges([...otherEdges, edge]);
};

const deleteNode = (graph, nodeId) => {
    const nodes = graph.getNodes().filter((node) => node.id !== nodeId);

    // Any nodes that have the node to be deleted listed as a successor or predecessor needs to be amended to remove
    // this reference.
    const successorNodeIds = new Set(
        graph
            .getEdges()
            .filter(({ source }) => source === nodeId)
            .map(({ target }) => target)
    );
    const predecessorNodeIds = new Set(
        graph
            .getEdges()
            .filter(({ target }) => target === nodeId)
            .map(({ source }) => source)
    );
    for (const node of nodes) {
        if (successorNodeIds.has(node.id)) {
            node.data.predecessors = node.data.predecessors.filter(
                (id) => id !== nodeId
            );
        } else if (predecessorNodeIds.has(node.id)) {
            node.data.successors = node.data.successors.filter(
                (id) => id !== nodeId
            );
        }
    }
    graph.setNodes(nodes);
};

const deleteEdge = (graph, edgeId) => {
    const edges = graph.getEdges();
    const edgeToDelete = edges.splice(
        edges.findIndex(({ id }) => id === edgeId),
        1
    )[0];
    graph.setEdges(edges);

    const { source: sourceNodeId, target: targetNodeId } = edgeToDelete;
    const sourceNode = graph.getNode(sourceNodeId);
    sourceNode.data.successors = sourceNode.data.successors.filter(
        (successor) => successor !== targetNodeId
    );

    const targetNode = graph.getNode(targetNodeId);
    targetNode.data.predecessors = targetNode.data.predecessors.filter(
        (predecessor) => predecessor !== sourceNodeId
    );
    delete targetNode.data.predecessorColumns[sourceNodeId];

    const otherNodes = graph
        .getNodes()
        .filter(({ id }) => id !== sourceNodeId && id !== targetNodeId);
    graph.setNodes([sourceNode, targetNode, ...otherNodes]);
};

const getNodeTypeColorClass = (nodeType) => {
    switch (nodeType) {
        case "pyspark":
            return "danger";
        case "pandas_on_spark":
            return "primary";
        case "pandas":
            return "success";
        case "spark_sql":
            return "info";
        default:
            return "warning";
    }
};

export {
    getPredecessorNodesAndEdges,
    addNodeAndPredecessors,
    refreshNodePositions,
    moveToNode,
    generateCodeTemplate,
    getNewNodeDef,
    consolidateEdge,
    deleteNode,
    deleteEdge,
    getNodeTypeColorClass,
};
