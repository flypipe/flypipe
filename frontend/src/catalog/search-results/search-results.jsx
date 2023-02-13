import React, {useState, useCallback, useMemo} from 'react';
import { ReactFlowProvider } from 'reactflow';
import NodeList from './node-list';
import NodeDetails from './node-details';
import Graph from '../../graph/graph';


const SearchResults = ({nodes}) => {
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
    return <ReactFlowProvider>
        <div className="d-flex col">
            <div className="d-flex flex-column col-4 m-4">
                <h3>{numberResultsText}</h3>
                <NodeList 
                    nodes={nodes} 
                    selectedNode={selectedNode}
                    handleSelectNode={handleSelectNode}
                />
            </div>
            <div className="col-7">
                <Graph nodeDefs={nodes}/>
                {/* <NodeDetails node={nodes.filter(({nodeKey}) => nodeKey === selectedNode)[0]}/> */}
            </div>
        </div>
    </ReactFlowProvider>
};

export default SearchResults;