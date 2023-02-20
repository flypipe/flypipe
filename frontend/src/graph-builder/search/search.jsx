import React, {useMemo, useState, useCallback} from 'react';
import NodeList from './node-list';
import Fuse from 'fuse.js';


const fuseOptions = {
    keys: ["name"]
};


const Search = ({nodes}) => {
    const fuse = useMemo(() => new Fuse(nodes, fuseOptions), [nodes]);
    const [searchResults, setSearchResults] = useState(nodes);
    const numberSearchResultsText = useMemo(() => searchResults.length === 1 ? `${searchResults.length} Result` : `${searchResults.length} Results`, [searchResults]);
    const onSearchChange = useCallback((e) => {
        const searchString = e.target.value;
        if (!searchString) {
            setSearchResults(nodes);
        } else {
            const rawSearchResults = fuse.search(searchString);
            setSearchResults(rawSearchResults.map(({item}) => item));
        }
    }, [setSearchResults]);

    return <>
        <div className="form-outline">
          <input type="search" className="form-control" placeholder="Search" aria-label="Search" onChange={onSearchChange} />
        </div>
        <br/>
        <h3>{numberSearchResultsText}</h3>
        <NodeList 
            nodes={searchResults} 
            allNodes={nodes}
        />
    </>
};


export default Search;
