import React, {useState, useMemo, useCallback, useEffect} from 'react';
import Node from './node';
import Pagination from './pagination';

// Maximum number of search entries per page
const SEARCH_PAGE_SIZE = 5;
// Maximum number of pages to show at the bottom of the screen
const SEARCH_PAGE_GROUP_SIZE = 3;


const NodeList = ({nodes}) => {
    const [currentNodes, setCurrentNodes] = useState([]);
    const [checkedNodes, setCheckedNodes] = useState(new Set());
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
    const handleCheck = useCallback((e) => {
        if (e.target.checked) {
            setCheckedNodes((prevCheckedNodes) => new Set([...prevCheckedNodes, e.target.value]));
        } else {
            setCheckedNodes((prevCheckedNodes) => new Set([...prevCheckedNodes].filter(x => x !== e.target.value)));
        }
        // TODO- USE A CONTEXT
    }, []);
    
    return <div>
        {currentNodes.map(
            ({name, importCmd, description}, i) => <Node key={`node-search-result-${i}`} name={name} importCmd={importCmd} description={description} checked={checkedNodes.has(name)} handleCheck={handleCheck}/>
        )}
        <Pagination maxPage={maxPage} pageGroupSize={SEARCH_PAGE_GROUP_SIZE} handleClickPage={handleChangePage}/>
    </div>
};

export default NodeList;