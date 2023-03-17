import { assignNodePositions, NODE_WIDTH, NODE_HEIGHT } from './util';


// Margin around the sides of a group subgraph
const GROUP_MARGIN = 50;


class Group {
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
        const nodes = this.graph.getNodes();
        const edges = this.graph.getEdges();
        const group = nodes.find(({id}) => id === this.groupId);
        const groupNodes = nodes.filter(({id}) => group.data.nodes.has(id));
        // All nodes within the group should be hidden if we are minimising and shown if maximising
        groupNodes.forEach(groupNode => {
            groupNode.hidden = isMinimise;
        })
        // Edges that are between nodes within the group
        const groupEdges = edges.filter(({source, target}) => group.data.nodes.has(source) && group.data.nodes.has(target));
        // All edges to and from nodes within the group should be hidden if we are minimising and shown if maximising
        edges.filter(({source, target}) => group.data.nodes.has(source) || group.data.nodes.has(target)).forEach(edge => edge.hidden=isMinimise);
        // All edges to and from the group node must be set to visible if minimising and hidden if maximising
        edges.filter(({source, target}) => source === group.id || target === group.id).forEach(edge => edge.hidden=!isMinimise);

        group.data.isMinimised = isMinimise;
        if (isMinimise) {
            group.style = {
                width: NODE_WIDTH,
                height: NODE_HEIGHT,
            }
        } else {
            assignNodePositions(groupNodes, groupEdges, {
                marginx: GROUP_MARGIN,
                marginy: GROUP_MARGIN,
            });
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
        }

        // Save changes
        this.graph.setNodes(nodes);
        this.graph.setEdges(edges);
    }
}

export default Group;