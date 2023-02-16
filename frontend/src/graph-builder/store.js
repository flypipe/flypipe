import { create } from 'zustand';
import { MarkerType } from 'reactflow';
import { assignNodePositions } from './util';


const NODE_WIDTH = 100;
const NODE_HEIGHT = 50;


const _addGraphData = (store, newNodes, newEdges) => {
    const {
        nodes: prevNodes, 
        edges: prevEdges, 
        nodeKeys: prevNodeKeys, 
        edgeKeys: prevEdgeKeys
    } = store;
    const nodesToAdd = newNodes.filter((node) => !prevNodeKeys.has(node.id)).map((node) => ({
        ...node,
        width: NODE_WIDTH,
        height: NODE_HEIGHT
    }));
    const edgesToAdd = newEdges.filter((edge) => !prevEdgeKeys.has(edge.id)).map(({source, target}) => ({
        id: `${source}-${target}`, 
        source,
        target,
        "markerEnd": {
            type: MarkerType.ArrowClosed,
        },
    }));
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

const _removeGraphData = (store, nodesToRemove, edgesToRemove) => {
    const {
        nodes: prevNodes, 
        edges: prevEdges, 
        nodeKeys: prevNodeKeys, 
        edgeKeys: prevEdgeKeys
    } = store;
    const nodes = prevNodes.filter(({id}) => !nodesToRemove.includes(id));
    const edges = prevEdges.filter(({id}) => !edgesToRemove.includes(id));
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
    editNode: (nodeId, changes) => set((prevStore) => {
        const store = {...prevStore}
        store.nodes = store.nodes.map((node) => {
            if (node.id === nodeId) {
                return {...node, ...changes};
            } else {
                return node;
            }
        })
        return store;
    }),
    addEdge: (source, target) => set((prevStore) => _addGraphData(prevStore, [], [{source, target}])),
    addNodesAndEdges: (newNodes, newEdges) => set((prevStore) => _addGraphData(prevStore, newNodes, newEdges)),
    removeNodesAndEdges: (nodesToRemove, edgesToRemove) => set((prevStore) => _removeGraphData(prevStore, nodesToRemove, edgesToRemove))
}));
