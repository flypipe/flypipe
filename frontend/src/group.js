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
        // All edges to and from nodes within the group should be hidden if we are minimising and shown if maximising
        edges
            .filter(
                ({ source, target }) =>
                    group.data.nodes.has(source) || group.data.nodes.has(target)
            )
            .forEach((edge) => (edge.hidden = isMinimise));
        // All edges to and from the group node must be set to visible if minimising and hidden if maximising
        // We also need to update the source/target name if to match the new group node name
        edges
            .filter(
                ({ source, target }) =>
                    source === group.id || target === group.id
            )
            .forEach((edge) => {
                edge.hidden = !isMinimise;
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
