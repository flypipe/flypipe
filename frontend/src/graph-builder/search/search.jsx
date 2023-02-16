import React, {useMemo, useState, useCallback} from 'react';
import NodeList from './node-list';


const Search = () => {
    const numberResultsText = useMemo(() => {
        if (nodes.length === 1) {
            return `${nodes.length} Result`
        } else {
            return `${nodes.length} Results`
        }
    }, [nodes]);
    const [selectedNode, setSelectedNode] = useState(nodes.length > 0 ? nodes[0].nodeKey : null);
    const handleSelectNode = useCallback((nodeKey) => {
        setSelectedNode(nodeKey);
    }, [setSelectedNode]);
    return <>
        <h3>{numberResultsText}</h3>
        <NodeList 
            nodes={nodes} 
            selectedNode={selectedNode}
            handleSelectNode={handleSelectNode}
        />
    </>
};


export default Search;