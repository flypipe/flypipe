import React, {useMemo} from 'react';
import NodeList from './node-list';
import NodeDetails from './node-details';


const SearchResults = ({nodes}) => {
    const numberResultsText = useMemo(() => {
        if (nodes.length === 1) {
            return `${nodes.length} Result`
        } else {
            return `${nodes.length} Results`
        }
    }, [nodes]);
    return <div className="d-flex col">
        <div className="d-flex flex-column col-4 m-4">
            <h3>{numberResultsText}</h3>
            <NodeList nodes={nodes}/>
        </div>
        <div className="col-8">
            <NodeDetails/>
        </div>
    </div>
};

export default SearchResults;