import { create } from 'zustand';
import { assignNodePositions } from './util';


export const useStore = create((set) => ({
    nodes: [],
    edges: [],
    nodeKeys: new Set(),
    edgeKeys: new Set(),
    addGraphData: (newNodes, newEdges) => set((prevStore) => {
        const {nodes: prevNodes, edges: prevEdges} = prevStore;
        const nodes = [...prevNodes, ...newNodes];
        const edges = [...prevEdges, ...newEdges];
        const newStore = {
            nodes,
            edges,
            nodeKeys: new Set(nodes.map(node => node.id)),
            edgeKeys: new Set(edges.map(edge => edge.id))
        };
        // Recalculate the positions of the nodes
        assignNodePositions(nodes, edges);
    
        return newStore;
    })
}));
