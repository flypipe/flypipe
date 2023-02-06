import React from 'react';
import NodeSearchResultItem from './node-search-result-item';


const NodeSearchResults = ({nodes}) => {
    console.log(`Rerendered node search results with ${nodes}`);
    return <div>
        {nodes.map(
            ({name, nodePath, description}, i) => <NodeSearchResultItem key={`node-search-result-${i}`} name={name} nodePath={nodePath} description={description}/>
        )}
    </div>

};

export default NodeSearchResults;