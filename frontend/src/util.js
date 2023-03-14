import dagre from "dagre";
import { ANIMATION_SPEED, DEFAULT_ZOOM } from "./graph/config";
import { MarkerType } from "reactflow";

// Fixed size of a regular node
export const NODE_WIDTH = 250;
export const NODE_HEIGHT = 75;
// Number of pixels between adjacent ranks in the x-axis for the dagre layout
export const NODE_RANK_SEPERATOR = 150;
// Margin around the sides of a group subgraph
export const GROUP_MARGIN = 50;

// Given the nodes & edges from the graph, construct a dagre graph to automatically assign
// the positions of the nodes.
const assignNodePositions = (nodes, edges) => {
    const dagreGraph = new dagre.graphlib.Graph();
    console.info("***Start dagre");
    dagreGraph.setGraph({
        rankdir: "LR",
        nodesep: 100,
        ranksep: 150,
        marginx: GROUP_MARGIN,
        marginy: GROUP_MARGIN,
    });
    dagreGraph.setDefaultEdgeLabel(() => ({}));
    const visibleNodes = nodes.filter(({ hidden }) => !hidden);
    visibleNodes.forEach(({ id, style }) => {
        const { width, height } = style;
        console.info(`Add node ${id} to dagre w,h=${width}, ${height}`);
        dagreGraph.setNode(id, { width, height });
    });

    edges.forEach(({ source, target }) => {
        console.info(`Add edge between ${source} & ${target} to dagre`);
        dagreGraph.setEdge(source, target);
    });
    dagre.layout(dagreGraph);

    visibleNodes.forEach((node) => {
        const nodeWithPosition = dagreGraph.node(node.id);
        const { width, height } = node.style;

        // We are shifting the dagre node position (anchor=center center) to the top left
        // so it matches the React Flow node anchor point (top left).
        node.position = {
            x: nodeWithPosition.x - width / 2,
            y: nodeWithPosition.y - height / 2,
        };
        console.log(
            `Position of node ${node.id}: ${node.position.x}, ${node.position.y}`
        );
    });
    console.info("***End dagre");
};

const refreshNodePositions = (graph) => {
    // Exclude new nodes that have no edges, the idea being that after a new node is dragged into the graph the
    // position remains under the user's discretion until it is connected into other nodes.
    const calculatedNodes = [];
    const ignoredNodes = [];
    graph.getNodes().forEach((node) => {
        if (
            node.type !== "group" &&
            node.data.isNew &&
            node.data.predecessors.length === 0 &&
            node.data.successors.length === 0
        ) {
            ignoredNodes.push(node);
        } else {
            calculatedNodes.push(node);
        }
    });

    const groups = calculatedNodes.filter(({ type }) => type === "group");
    const nodes = calculatedNodes.filter(({ type }) => type !== "group");
    groups.forEach((group) => {
        const groupNodes = nodes.filter(({ id }) => group.data.nodes.has(id));
        const groupEdges = graph
            .getEdges()
            .filter(
                ({ source, target }) =>
                    group.data.nodes.has(source) && group.data.nodes.has(target)
            );
        assignNodePositions(groupNodes, groupEdges);
        group.position = {
            x: group.position.x,
            y: group.position.y,
        };
        group.style = {
            width: Math.max(
                ...groupNodes.map(
                    (groupNode) =>
                        groupNode.position.x +
                        groupNode.style.width +
                        GROUP_MARGIN
                )
            ),
            height: Math.max(
                ...groupNodes.map(
                    (groupNode) =>
                        groupNode.position.y +
                        groupNode.style.height +
                        GROUP_MARGIN
                )
            )
        };
        console.log(
            `Group ${group.id} has width ${group.style.width} and height ${group.style.height}`
        );
    });
    const groupsAndUngroupedNodes = calculatedNodes.filter(
        (node) => node.type === "group" || !node.data.group
    );
    assignNodePositions(
        groupsAndUngroupedNodes,
        graph
            .getEdges()
            .filter(
                ({ source, target }) =>
                    groupsAndUngroupedNodes
                        .map(({ id }) => id)
                        .includes(source) &&
                    groupsAndUngroupedNodes.map(({ id }) => id).includes(target)
            )
    );
    graph.setNodes([...calculatedNodes, ...ignoredNodes]);
};

const refreshGroups = (graph) => {
    const nodes = graph.getNodes().filter(({ type }) => type !== "group");
    const groups = [...graph.getNodes().filter(({ type }) => type === "group")];

    const edges = graph.getEdges();

    // Handle group expansion/minimisation
    groups.forEach((group) => {
        if (group.data.isExpanded) {
            group.data.nodes.forEach((nodeId) => {
                const node = nodes.find((n) => n.id === nodeId);
                node.hidden = false;
            });
            edges
                .filter(
                    ({ source, target }) =>
                        source === group.id || target === group.id
                )
                .forEach((edge) => {
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
            group.data.nodes.forEach((nodeId) => {
                const node = nodes.find((n) => n.id === nodeId);
                node.hidden = true;
                edges
                    .filter(
                        ({ source, target }) =>
                            source === nodeId || target === nodeId
                    )
                    .forEach((edge) => {
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
        extent: "parent",
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
        },
    };
};

// Given an input node, get the list of nodes and edges of all of the input node's predecessors.
const getPredecessorNodesAndEdges = (nodeDefs, nodeKey) => {
    const nodeDef = nodeDefs.find((nodeDef) => nodeDef.nodeKey === nodeKey);
    const frontier = [...nodeDef.predecessors];
    const selectedNodeDefs = [nodeDef];
    const edges = [];
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
                edges.push([current.nodeKey, successorKey]);
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

const getEdgeDef = (graph, source, target) => {
    const successor = graph.getNode(target);
    return {
        id: `${source}-${target}`,
        isNew: successor.data.isNew || false,
        source,
        target,
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
};

const addOrReplaceEdge = (graph, edge) => {
    console.info("Adding or replacing edge");
    console.info(edge);
    const edges = graph.getEdges();
    const otherEdges = edges.filter(({ id }) => id !== edge.id);
    graph.setEdges([edge, ...otherEdges]);

    const nodes = graph.getNodes();
    const sourceNode = nodes.find(({ id }) => id === edge.source);
    const targetNode = nodes.find(({ id }) => id === edge.target);
    if (sourceNode.type !== "group" && targetNode.type !== "group") {
        // Update the predecessor/successor fields on the nodes the edge is going between
        if (!sourceNode.data.successors.includes(targetNode.id)) {
            sourceNode.data.successors.push(targetNode.id);
        }
        if (!targetNode.data.predecessors.includes(sourceNode.id)) {
            targetNode.data.predecessors.push(sourceNode.id);
        }
        targetNode.data.predecessorColumns = {
            ...targetNode.data.predecessorColumns,
            [sourceNode.id]: [],
        };

        // If the source and/or the target node have groups and they aren't the same group then we need to create an
        // extra hidden edge to the group node
        const sourceGroup = sourceNode.data.group;
        const targetGroup = targetNode.data.group;
        if (sourceGroup !== targetGroup) {
            const source = sourceGroup || sourceNode.id;
            const target = targetGroup || targetNode.id;
            addOrReplaceEdge(graph, getEdgeDef(graph, source, target));
        }
        graph.setNodes(nodes);
    }
};

const addNodeAndPredecessors = (graph, nodeDefs, nodeKey) => {
    // Add a node to the graph, we also add any ancestor nodes/edges
    const [predecessorNodes, predecessorEdges] = getPredecessorNodesAndEdges(
        nodeDefs,
        nodeKey
    );

    // Add the node and any predecessor nodes/edges to the graph that aren't already there.
    const currentNodes = new Set(graph.getNodes().map(({ id }) => id));
    const currentEdges = new Set(graph.getEdges().map(({ id }) => id));
    const newNodes = predecessorNodes.filter(({ id }) => !currentNodes.has(id));
    const nodes = graph.getNodes().filter(({ type }) => type !== "group");
    const groups = graph.getNodes().filter(({ type }) => type === "group");

    newNodes.forEach((node) => {
        const groupId = node.data.group;
        if (groupId) {
            const group = groups.find(({ id }) => id === groupId);
            group.data.nodes.add(node.id);
            group.hidden = false;
        }
    });
    graph.setNodes([...groups, ...nodes, ...newNodes]);
    predecessorEdges.forEach((edgeDef) => {
        const [source, target] = edgeDef;
        addOrReplaceEdge(graph, getEdgeDef(graph, source, target));
    });

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
    addNodeAndPredecessors,
    refreshNodePositions,
    moveToNode,
    generateCodeTemplate,
    getNewNodeDef,
    getEdgeDef,
    addOrReplaceEdge,
    deleteNode,
    deleteEdge,
    getNodeTypeColorClass,
};
