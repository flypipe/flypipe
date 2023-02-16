import { create } from 'zustand';
import { assignNodePositions } from './util';


const _addGraphData = (store, newNodes, newEdges) => {
    const {
        nodes: prevNodes, 
        edges: prevEdges, 
        nodeKeys: prevNodeKeys, 
        edgeKeys: prevEdgeKeys
    } = store;
    const nodesToAdd = newNodes.filter((node) => !prevNodeKeys.has(node.id));
    const edgesToAdd = newEdges.filter((edge) => !prevEdgeKeys.has(edge.id));
    if (!nodesToAdd && !edgesToAdd) {
        // Return the original store if we have nothing to add
        return store;
    }
    const nodes = [...prevNodes, ...nodesToAdd];
    const edges = [...prevEdges, ...edgesToAdd];
    const newStore = {
        nodes,
        edges,
        nodeKeys: new Set(nodes.map(node => node.id)),
        edgeKeys: new Set(edges.map(edge => edge.id))
    };
    // Recalculate the positions of the nodes
    assignNodePositions(nodes, edges);

    return newStore;
};


export const useStore = create((set) => ({
    nodes: [],
    edges: [],
    nodeKeys: new Set(),
    edgeKeys: new Set(),
    addNode: (newNode) => set((prevStore) => _addGraphData(prevStore, [newNode], [])),
    addEdge: (newEdge) => set((prevStore) => _addGraphData(prevStore, [], [newEdge])),
    addNodesAndEdges: (newNodes, newEdges) => set((prevStore) => _addGraphData(prevStore, newNodes, newEdges))
}));
