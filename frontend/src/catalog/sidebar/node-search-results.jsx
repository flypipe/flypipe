import React from 'react';
import NodeSearchResultItem from './node-search-result-item';


const NodeSearchResults = ({nodes}) => {
    return <div className="mx-4 mb-4">
        {nodes.map(
            ({name, nodePath, description}, i) => <NodeSearchResultItem key={`node-search-result-${i}`} name={name} nodePath={nodePath} description={description}/>
        )}
    </div>

};

export default NodeSearchResults;