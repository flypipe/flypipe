import dagre from "dagre";
import { ANIMATION_SPEED, DEFAULT_ZOOM } from "./graph/config";
import { MarkerType } from "reactflow";
import { Group, getGroup } from "./group";

// Fixed size of a regular node
export const NODE_WIDTH = 250;
export const NODE_HEIGHT = 75;
// Number of pixels between adjacent ranks in the x-axis for the dagre layout
export const NODE_RANK_SEPERATOR = 150;

// Given the nodes & edges from the graph, construct a dagre graph to automatically assign
// the positions of the nodes.
const assignNodePositions = (nodes, edges, dagreOptions = {}) => {
    const dagreGraph = new dagre.graphlib.Graph();
    dagreGraph.setGraph({
        rankdir: "LR",
        nodesep: 100,
        ranksep: 150,
        ...dagreOptions,
    });
    dagreGraph.setDefaultEdgeLabel(() => ({}));
    nodes.forEach(({ id, style }) => {
        const { width, height } = style;
        dagreGraph.setNode(id, { width, height });
    });

    edges.forEach(({ source, target }) => {
        dagreGraph.setEdge(source, target);
    });
    dagre.layout(dagreGraph);

    nodes.forEach((node) => {
        const nodeWithPosition = dagreGraph.node(node.id);
        const { width, height } = node.style;

        // We are shifting the dagre node position (anchor=center center) to the top left
        // so it matches the React Flow node anchor point (top left).
        node.position = {
            x: nodeWithPosition.x - width / 2,
            y: nodeWithPosition.y - height / 2,
        };
    });
};

const refreshNodePositions = (graph) => {
    // Exclude new nodes that have no edges, the idea being that after a new node is dragged into the graph the
    // position remains under the user's discretion until it is connected into other nodes.
    graph
        .getNodes()
        .filter(({ type }) => type === "flypipe-group")
        .forEach(({ id }) => {
            const group = new Group(graph, id);
            group.refresh();
        });

    const nodes = graph.getNodes();
    const ignoredNodes = nodes.filter(
        (node) =>
            node.data.isNew &&
            node.data.predecessors.length === 0 &&
            node.data.successors.length === 0
    );
    const topLevelNodes = nodes.filter((node) => {
        return (
            node.type === "flypipe-group" ||
            (!node.data.group &&
                !ignoredNodes.map(({ id }) => id).includes(node.id))
        );
    });
    const topLevelNodeIds = topLevelNodes.map(({ id }) => id);

    assignNodePositions(
        topLevelNodes,
        graph
            .getEdges()
            .filter(
                ({ source, target }) =>
                    topLevelNodeIds.includes(source) &&
                    topLevelNodeIds.includes(target)
            )
    );
    graph.setNodes(nodes);
};

// Retrieve the graph node representation of a node
const convertNodeDefToGraphNode = (graph, nodeDef, isNew = true) => {
    const group = nodeDef.group ? getGroup(graph, nodeDef.group) : null;
    return {
        id: nodeDef.nodeKey,
        type: isNew ? "flypipe-node-new" : "flypipe-node-existing",
        parentNode: group?.id || "",
        ...(nodeDef.group && { extent: "parent" }),
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
const getPredecessorNodesAndEdges = (graph, nodeDefs, nodeKey) => {
    const nodeDef = nodeDefs.find((nodeDef) => nodeDef.nodeKey === nodeKey);
    const frontier = [...nodeDef.predecessors];
    const selectedNodeDefs = [nodeDef];
    const addedKeys = [nodeDef.nodeKey];
    const mapOfEdges = new Map();
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
                const edgeKey = current.nodeKey + "-" + successorKey;
                if (!mapOfEdges.has(edgeKey)) {
                    mapOfEdges.set(edgeKey, [current.nodeKey, successorKey]);
                }
            }
        }
        for (const predecessor of current.predecessors) {
            // In a graph of A->[B, C]->D, B and C will cause to add A to the frontier twice
            // in this case, do not add if A has already been added
            if (!frontier.includes(predecessor)) {
                frontier.push(predecessor);
            }
        }
    }
    const edges = Array.from(mapOfEdges.values());
    return [
        selectedNodeDefs.map((nodeDef) =>
            convertNodeDefToGraphNode(graph, nodeDef, false)
        ),
        edges,
    ];
};

const getEdgeDef = (graph, source, target, linkedEdge = null) => {
    const targetNode = graph.getNode(linkedEdge ? linkedEdge.target : target);
    const sourceNode = graph.getNode(linkedEdge ? linkedEdge.source : source);
    return {
        id: `${source}-${target}`,
        isNew: targetNode.data.isNew || false,
        source,
        target,
        ...(linkedEdge && { linkedEdge: linkedEdge.id }),
        markerEnd: {
            type: MarkerType.ArrowClosed,
            width: 15,
            height: 15,
        },
        ...(targetNode.data.isActive || {
            style: {
                strokeDasharray: "5,5",
            },
        }),
    };
};

const addOrReplaceEdge = (graph, edge) => {
    const edges = graph.getEdges();
    const otherEdges = edges.filter(
        ({ source, target }) => source !== edge.source || target !== edge.target
    );
    graph.setEdges([edge, ...otherEdges]);

    const nodes = graph.getNodes();
    const sourceNode = nodes.find(({ id }) => id === edge.source);
    const targetNode = nodes.find(({ id }) => id === edge.target);
    if (
        sourceNode.type !== "flypipe-group" &&
        targetNode.type !== "flypipe-group"
    ) {
        // Update the predecessor/successor fields on the nodes the edge is going between
        if (!sourceNode.data.successors.includes(targetNode.id)) {
            sourceNode.data.successors.push(targetNode.id);
        }
        if (!targetNode.data.predecessors.includes(sourceNode.id)) {
            targetNode.data.predecessors.push(sourceNode.id);
        }

        // If the source and/or the target node have groups and they aren't the same group then we need to create an
        // extra hidden edge to the group node
        const sourceGroup = sourceNode.data.group || null;
        const targetGroup = targetNode.data.group || null;
        if (sourceGroup !== targetGroup) {
            const source = sourceGroup
                ? getGroup(graph, sourceGroup).id
                : sourceNode.id;
            const target = targetGroup
                ? getGroup(graph, targetGroup).id
                : targetNode.id;
            addOrReplaceEdge(graph, getEdgeDef(graph, source, target, edge));

            // If exists source and target groups, needs to create edges of all internal nodes of sourceGroup to
            // targetGroup and edges from all internal nodes from targetGroup to sourceGroup
            if (
                sourceGroup != null &&
                targetGroup != null &&
                sourceGroup !== targetGroup
            ) {
                const sourceGroupNode = getGroup(graph, sourceGroup);
                const targetGroupNode = getGroup(graph, targetGroup);
                const internalTargetNodes = nodes.filter(
                    (node) => node.data.group === targetGroup
                );
                const internalSourceNodes = nodes.filter(
                    (node) => node.data.group === sourceGroup
                );

                internalTargetNodes.forEach((internalTargetNode) => {
                    internalTargetNode.data.predecessors.forEach(
                        (predecessorNodeId) => {
                            if (
                                sourceGroupNode.data.nodes.has(
                                    predecessorNodeId
                                )
                            ) {
                                const edgeToTargetDefinition = getEdgeDef(
                                    graph,
                                    source,
                                    internalTargetNode.id,
                                    edge
                                );
                                addOrReplaceEdge(graph, edgeToTargetDefinition);

                                const edgeFromSourceDefinition = getEdgeDef(
                                    graph,
                                    predecessorNodeId,
                                    target,
                                    edge
                                );
                                addOrReplaceEdge(
                                    graph,
                                    edgeFromSourceDefinition
                                );
                            }
                        }
                    );
                });
            }
        }
        graph.setNodes(nodes);
    }
};

const addNodeAndPredecessors = (graph, nodeDefs, nodeKey) => {
    // Add a node to the graph, we also add any ancestor nodes/edges
    const [predecessorNodes, predecessorEdges] = getPredecessorNodesAndEdges(
        graph,
        nodeDefs,
        nodeKey
    );

    // Add the node and any predecessor nodes/edges to the graph that aren't already there.
    const currentNodes = new Set(graph.getNodes().map(({ id }) => id));
    const newNodes = predecessorNodes.filter(({ id }) => !currentNodes.has(id));
    const nodes = graph
        .getNodes()
        .filter(({ type }) => type !== "flypipe-group");
    const groups = graph
        .getNodes()
        .filter(({ type }) => type === "flypipe-group");

    newNodes.forEach((node) => {
        const groupId = node.data.group;
        if (groupId) {
            const group = groups.find(
                ({ id }) => id === getGroup(graph, groupId).id
            );
            group.data.nodes.add(node.id);
            group.hidden = false;
        }
    });
    graph.setNodes([...groups, ...nodes, ...newNodes]);
    predecessorEdges.forEach((edgeDef) => {
        const [source, target] = edgeDef;
        addOrReplaceEdge(graph, getEdgeDef(graph, source, target));
    });

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

    const tagList = tags.map(({ name }) => `'${name}'`).join(", ");
    const dependencyList = predecessors
        .map((nodeId) => {
            const name = graph.getNode(nodeId).data.name;
            if (
                predecessorColumns[nodeId] !== undefined &&
                predecessorColumns[nodeId].length > 0
            ) {
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
    hasCache,
    cacheIsDisabled,
    hasProvidedInput,
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
    hasCache: hasCache || false,
    cacheIsDisabled: cacheIsDisabled || false,
    hasProvidedInput: hasProvidedInput || false,
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
        if (node.type === "flypipe-group") {
            // Groups have no predecessor/successor so we skip over them
            continue;
        }
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
    refreshNodePositions(graph);
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
    refreshNodePositions(graph);
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
    assignNodePositions,
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
