import React, {useState, useCallback} from 'react';
import SideBar from './sidebar/sidebar';
import Details from './details';
import SearchBar from './searchbar/searchbar';
import Header from './header';


const Catalog = () => {
    const [searchResultNodes, setSearchResultNodes] = useState(nodes);
    const handleUpdateSearch = useCallback((results) => {
        setSearchResultNodes(results);
    }, []);
    return <>
        <Header/>
        <div className="d-flex">
            <SideBar nodes={searchResultNodes}/>
            <Details/>
            <SearchBar nodes={nodes} handleUpdateSearch={handleUpdateSearch}/>
        </div>
    </>
}

export default Catalog;