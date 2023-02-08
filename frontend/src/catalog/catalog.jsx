import React, {useState, useCallback} from 'react';
import SideBar from './sidebar/sidebar';
import Details from './details';
import SearchBar from './searchbar/searchbar';
import Header from './header';
import CountBar from './count-bar/count-bar';


const Catalog = () => {
    const [searchResultNodes, setSearchResultNodes] = useState(nodes);
    const handleUpdateSearch = useCallback((results) => {
        setSearchResultNodes(results);
    }, []);
    return <>
        <Header/>
        <CountBar countBoxDefs={count_boxes}/>
        <div className="d-flex">
            <SideBar nodes={searchResultNodes}/>
            <Details/>
            <SearchBar nodes={nodes} handleUpdateSearch={handleUpdateSearch}/>
        </div>
    </>
}

export default Catalog;