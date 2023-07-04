import { assignNodePositions, NODE_WIDTH, NODE_HEIGHT } from "./util";

// Margin around the sides of a group subgraph
const GROUP_MARGIN = 50;

export const getGroup = (graph, groupId) => {
    const possibleIds = [
        groupId,
        `${groupId}-minimised`,
        `${groupId}-maximised`,
    ];
    let group = null;
    for (const possibleId of possibleIds) {
        group = graph.getNode(possibleId);
        if (group != null) {
            break;
        }
    }
    if (group == null) {
        throw new Error(`Unable to find group id ${groupId}`);
    }
    return group;
};

export class Group {
    // Class that handle groups and associated operations on them
    constructor(graph, groupId) {
        this.graph = graph;
        this.groupId = groupId;
    }

    refresh() {
        const group = this.graph.getNode(this.groupId);
        this._setMinimiseMaximise(group.data.isMinimised);
    }

    _setMinimiseMaximise(isMinimise) {
        // Minimise or maximise the group. On maximise, we:
        // - Make all the nodes inside a group visible
        // - Make all the edges associated with these nodes visible
        // - Hide all the edges associated with the group node
        // - Increase the size of the group node to encompass all the group's nodes
        // On minimise, we:
        // - Make all the nodes inside a group invisible
        // - Hide all the edges associated with these nodes
        // - Make visible all the edges associated with the group node
        // - Decrease the size of the group node to be the size of a regular node
        // Note- due to an annoying bug in Databricks we cannot change the size of an existing node. Therefore, we
        // instead add a completely new group node with a different id, and update any references to it.
        const newGroupId = [
            ...this.groupId.split("-").slice(0, -1),
            isMinimise ? "minimised" : "maximised",
        ].join("-");
        const nodes = this.graph.getNodes();
        const edges = this.graph.getEdges();
        const group = nodes.find(({ id }) => id === this.groupId);
        const otherNodes = nodes.filter(({ id }) => id !== this.groupId);
        const groupNodes = nodes.filter(({ id }) => group.data.nodes.has(id));

        //console.log("----- GROUP", group.data.label, isMinimise, "-----");

        // All nodes within the group should be hidden if we are minimising and shown if maximising
        groupNodes.forEach((groupNode) => {
            groupNode.hidden = isMinimise;
            groupNode.parentNode = newGroupId;
        });
        // Edges that are between nodes within the group
        const groupEdges = edges.filter(
            ({ source, target }) =>
                group.data.nodes.has(source) && group.data.nodes.has(target)
        );
        // Show or hide internal edges in group
        groupEdges.forEach((edge) => (edge.hidden = isMinimise));

        // We also need to update the source/target name if to match the new group node name
        edges
            .filter(
                ({ source, target }) =>
                    source === group.id || target === group.id
            )
            .forEach((edge) => {
                //console.log("");
                const source_node = nodes.find(({ id }) => id === edge.source);
                const source_node_is_group =
                    source_node.type === "flypipe-group";
                const source_group_node = source_node_is_group
                    ? source_node
                    : source_node.data.group
                    ? getGroup(this.graph, source_node.data.group)
                    : null;

                const target_node = nodes.find(({ id }) => id === edge.target);
                const target_node_is_group =
                    target_node.type === "flypipe-group";
                const target_group_node = target_node_is_group
                    ? target_node
                    : target_node.data.group
                    ? getGroup(this.graph, target_node.data.group)
                    : null;

                //console.log("Edge:", edge.source, "->", edge.target);
                edge.hidden = true;
                if (source_node_is_group && target_node_is_group) {
                    //console.log("case 0", edge);
                    if (
                        source_group_node.data.isMinimised &&
                        target_group_node.data.isMinimised
                    ) {
                        //console.log("case 0.1", edge);
                        edge.hidden = false;
                    } else {
                        //console.log("case 0.2", edge);
                        edge.hidden = true;

                        if (
                            source_group_node.data.isMinimised &&
                            !target_group_node.data.isMinimised
                        ) {
                            // show all edges from source group and internal nodes of target group
                            const internalTargetNodes = nodes.filter(
                                (node) =>
                                    node.data.group ===
                                    target_group_node.data.label
                            );
                            internalTargetNodes.forEach(
                                (internalTargetNode) => {
                                    internalTargetNode.data.predecessors.forEach(
                                        (predecessorNodeId) => {
                                            const predecessor_node = nodes.find(
                                                ({ id }) =>
                                                    id === predecessorNodeId
                                            );
                                            if (
                                                predecessor_node.data.group ===
                                                source_group_node.data.label
                                            ) {
                                                edges
                                                    .filter(
                                                        ({
                                                            source,
                                                            target,
                                                        }) => {
                                                            return (
                                                                source ==
                                                                    source_group_node.id &&
                                                                target ==
                                                                    internalTargetNode.id
                                                            );
                                                        }
                                                    )
                                                    .forEach((edge) => {
                                                        //console.log("case 0.2.1", edge);
                                                        edge.hidden = false;
                                                    });
                                            }
                                        }
                                    );
                                }
                            );
                        }
                    }
                } else if (!source_node_is_group) {
                    if (target_group_node.data.isMinimised) {
                        //console.log("case 1: source is node, target is group");
                        // show edge from non-group node to group node
                        if (source_group_node == null) {
                            //console.log("case 1.1", edge);
                            edge.hidden = false;
                        } else if (source_group_node.data.isMinimised) {
                            if (target_group_node.data.isMinimised) {
                                //console.log("case 1.2.1", edge);
                                edge.hidden = true;
                            } else {
                                //console.log("case 1.2.2", edge);
                                edge.hidden = false;
                            }
                        } else if (target_group_node == null) {
                            //console.log("case 1.3", edge);
                            edge.hidden = false;
                        } else if (target_group_node.data.isMinimised) {
                            //console.log("case 1.4", edge);
                            edge.hidden = false;
                        }

                        // hide all edges from source internal nodes to target_group
                        const internalTargetNodes = nodes.filter(
                            (node) =>
                                node.data.group === target_group_node.data.label
                        );
                        internalTargetNodes.forEach((internalTargetNode) => {
                            internalTargetNode.data.predecessors.forEach(
                                (predecessorNodeId) => {
                                    if (predecessorNodeId == source_node.id) {
                                        edges
                                            .filter(({ source, target }) => {
                                                return (
                                                    source == source_node.id &&
                                                    target ==
                                                        internalTargetNode.id
                                                );
                                            })
                                            .forEach((edge) => {
                                                //console.log("case 1.5:", edge);
                                                edge.hidden = true;
                                            });
                                    }
                                }
                            );
                        });
                    } else {
                        //console.log("case 2: source is group, target group is maximised", edge);
                        // hide edge from non-group node to group node
                        edge.hidden = true;

                        if (source_node.data.group == null) {
                            //source node does not bellong to a group
                            // show all edges from non-group node to internal nodes of group node
                            const internalTargetNodes = nodes.filter(
                                (node) =>
                                    node.data.group ===
                                    target_group_node.data.label
                            );
                            internalTargetNodes.forEach(
                                (internalTargetNode) => {
                                    internalTargetNode.data.predecessors.forEach(
                                        (predecessorNodeId) => {
                                            if (
                                                predecessorNodeId ==
                                                source_node.id
                                            ) {
                                                edges
                                                    .filter(
                                                        ({
                                                            source,
                                                            target,
                                                        }) => {
                                                            return (
                                                                source ==
                                                                    source_node.id &&
                                                                target ==
                                                                    internalTargetNode.id
                                                            );
                                                        }
                                                    )
                                                    .forEach((edge) => {
                                                        //console.log("case 2.1 source node do not belong to a group:", source_group_node, edge);
                                                        edge.hidden = false;
                                                    });
                                            }
                                        }
                                    );
                                }
                            );
                        } else if (!source_group_node.data.isMinimised) {
                            // show all edges from target group internal nodes that has source node as predecessor
                            const internalTargetNodes = nodes.filter(
                                (node) =>
                                    node.data.group ===
                                    target_group_node.data.label
                            );
                            internalTargetNodes.forEach(
                                (internalTargetNode) => {
                                    if (
                                        internalTargetNode.data.predecessors.includes(
                                            source_node.id
                                        )
                                    ) {
                                        edges
                                            .filter(({ source, target }) => {
                                                return (
                                                    source == source_node.id &&
                                                    target ==
                                                        internalTargetNode.id
                                                );
                                            })
                                            .forEach((edge) => {
                                                //console.log("case 2.2 source node belong to a group:", edge);
                                                edge.hidden = false;
                                            });
                                    }
                                }
                            );
                        }
                    }
                } else {
                    //console.log("case 3: source is group, target is node");
                    const target_node_is_visible = !target_node.hidden;

                    if (target_node_is_visible) {
                        //console.log("case 3.1", edge);
                        edge.hidden = false;
                    } else {
                        //console.log("case 3.2", edge);
                        edge.hidden = true;
                    }

                    if (source_group_node.data.isMinimised) {
                        //console.log("case 3.3");

                        if (target_group_node != null) {
                            if (target_group_node.data.isMinimised) {
                                //console.log("case 3.3.1", edge);
                                edge.hidden = true;
                            } else {
                                //console.log("case 3.3.2", edge);
                                edge.hidden = false;
                            }
                        } else {
                            //console.log("case 3.3.3", edge);
                            edge.hidden = false;
                        }

                        //hide all edges from source internal nodes
                        const internalSourceNodes = nodes.filter(
                            (node) =>
                                node.data.group === source_group_node.data.label
                        );
                        internalSourceNodes.forEach((internalSourceNode) => {
                            if (
                                target_node.data.predecessors.includes(
                                    internalSourceNode.id
                                )
                            ) {
                                edges
                                    .filter(({ source, target }) => {
                                        return (
                                            source == internalSourceNode.id &&
                                            target == target_node.id
                                        );
                                    })
                                    .forEach((edge) => {
                                        //console.log("case 3.3.4", edge);
                                        edge.hidden = true;
                                    });
                            }
                        });
                    } else {
                        //console.log("case 4.1");
                        if (target_group_node != null) {
                            //console.log("case 4.2", edge);
                            edge.hidden = true;
                        } else {
                            //console.log("case 4.3", edge);
                            edge.hidden = true;

                            //show all edges from internal nodes of source group to target node
                            const internalSourceNodes = nodes.filter(
                                (node) =>
                                    node.data.group ===
                                    source_group_node.data.label
                            );
                            internalSourceNodes.forEach(
                                (internalSourceNode) => {
                                    if (
                                        target_node.data.predecessors.includes(
                                            internalSourceNode.id
                                        )
                                    ) {
                                        edges
                                            .filter(({ source, target }) => {
                                                return (
                                                    source ==
                                                        internalSourceNode.id &&
                                                    target == target_node.id
                                                );
                                            })
                                            .forEach((edge) => {
                                                //console.log("case 4.3.1", edge);
                                                edge.hidden = false;
                                            });
                                    }
                                }
                            );
                        }
                    }
                }
                //__main___function_t01_t01-__main___function_between_groups_between_groups
                //__main___function_t01_t01-__main___function_between_groups_between_groups
                if (edge.source === group.id) {
                    edge.source = newGroupId;
                } else {
                    edge.target = newGroupId;
                }
            });

        if (!isMinimise) {
            assignNodePositions(groupNodes, groupEdges, {
                marginx: GROUP_MARGIN,
                marginy: GROUP_MARGIN,
            });
        }
        const newGroup = {
            ...group,
            id: newGroupId,
            data: {
                ...group.data,
                isMinimised: isMinimise,
            },
            hidden: groupNodes.length === 0,
            style:
                isMinimise || groupNodes.length === 0
                    ? {
                          width: NODE_WIDTH,
                          height: NODE_HEIGHT,
                      }
                    : {
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
                          ),
                      },
        };
        this.groupId = newGroupId;

        // Save changes
        this.graph.setNodes([...otherNodes, newGroup]);
        this.graph.setEdges(edges);
    }
}
