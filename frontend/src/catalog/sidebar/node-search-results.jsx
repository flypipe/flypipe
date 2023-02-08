import React, {useState, useMemo, useCallback, useEffect} from 'react';
import NodeSearchResultItem from './node-search-result-item';
import Pagination from './pagination';

// Maximum number of search entries per page
const SEARCH_PAGE_SIZE = 5;
// Maximum number of pages to show at the bottom of the screen
const SEARCH_PAGE_GROUP_SIZE = 2;



const NodeSearchResults = ({nodes}) => {
    const [currentNodes, setCurrentNodes] = useState([]);
    // nodes can change via the node search panel, we must reset the nodes if so
    useEffect(() => {
        setCurrentNodes(nodes.slice(0, SEARCH_PAGE_SIZE))
    }, [nodes]);

    const maxPage = useMemo(() => Math.ceil(nodes.length / SEARCH_PAGE_SIZE), [nodes]);
    const nodesByPage = useMemo(() => {
        const result = {};
        for (let i=1; i<maxPage + 1; i++) {
            result[i] = nodes.slice((i-1) * SEARCH_PAGE_SIZE, i * SEARCH_PAGE_SIZE);
        }
        return result;
    }, [nodes]);
    const handleChangePage = useCallback((pageNumber) => {
        setCurrentNodes(nodesByPage[pageNumber]);
    }, [nodesByPage]);
    
    return <div className="mx-4 mb-4">
        {currentNodes.map(
            ({name, importCmd, description}, i) => <NodeSearchResultItem key={`node-search-result-${i}`} name={name} importCmd={importCmd} description={description}/>
        )}
        <Pagination maxPage={maxPage} pageGroupSize={SEARCH_PAGE_GROUP_SIZE} handleClickPage={handleChangePage}/>
    </div>

};

export default NodeSearchResults;