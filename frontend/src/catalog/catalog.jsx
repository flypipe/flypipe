import React, {useState, useCallback} from 'react';
import Search from './search/search';
import SearchResults from './search-results/search-results';
import Header from './header';


const Catalog = () => {
    const [searchResultNodes, setSearchResultNodes] = useState(nodes);
    const handleUpdateSearch = useCallback((results) => {
        setSearchResultNodes(results);
    }, []);
    return <>
        <Header/>
        <div className="d-flex">
            <Search nodes={nodes} handleUpdateSearch={handleUpdateSearch}/>
            <SearchResults nodes={searchResultNodes}/>
        </div>
    </>
}

export default Catalog;